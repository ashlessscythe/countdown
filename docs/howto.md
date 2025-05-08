# Python Tool for Processing Shipment and Delivery Snapshots

## Introduction

This task involves building a Python tool to consolidate shipment **serial-level data** and delivery **package-level data** from two sets of Excel snapshots. The goal is to produce an **aggregated view per user** of their scanning progress (picked vs shipped packages) and delivery status, and update this view at a regular interval. We have two data sources:

- **ZMDESNR** – an Excel snapshot containing serial-level shipment status records (each row likely corresponds to a serial number scan, including who scanned it, when, its status, pallet ID, delivery number, etc.).
- **VL06O** – an Excel snapshot containing delivery-level info (each row per delivery, including total number of packages in that delivery, shipping point, etc.).

A configuration file (`config.py`) provides base directory paths and constants:

```python
BASE_DIR = 'C:/temp/reports'
SERIAL_NUMBERS_DIR = os.path.join(BASE_DIR, 'ZMDESNR')
DELIVERY_INFO_DIR = os.path.join(BASE_DIR, 'VL06O')
OUT_DIR = os.path.join(BASE_DIR, 'parquet_output')
WAREHOUSE_FILTER = 'E01'
STATUS_MAPPING = {'ASH': 'picked', 'SHP': 'shipped / closed'}
INTERVAL = 60  # seconds (default update interval)
```

**Key Requirements:** We must always process **only the latest snapshot** file from each directory, filter records to warehouse `E01` only, and map status codes `'ASH'` → “picked” and `'SHP'` → “shipped / closed”. If the same serial was previously “picked” and later “shipped”, we consider it shipped (no double-counting). The tool should compare each new snapshot’s processed data to the last cached results to **detect changes**, output the aggregated data in a structured format (JSON or Parquet), and retain only the **last 5 output files** to avoid clutter. The output will be used by a dashboard, with one entry per **user + delivery** showing:

- **user** (scanner’s ID)
- **delivery** (delivery number)
- **scanned packages** (count of packages this user scanned for that delivery)
- **delivery total packages** (total packages in that delivery, from VL06O)
- **status breakdown** (how many of the scanned packages are picked vs shipped)
- **last scan time** (timestamp of the user’s most recent scan)
- **time between scans** (time difference between the user’s last two scans)

Below we outline the design and then present a modular Python script that implements this functionality with logging and error handling.

## Step 1: Retrieve the Latest Snapshot Files

For each source directory (`ZMDESNR` and `VL06O`), the tool will identify the newest Excel file and load it. The snapshot files are timestamped in their filenames (e.g. `20250508165553_ZMDESNR.xlsx`), so sorting by filename or modification time can identify the latest. We can use Python’s `os` or `pathlib` to list files and pick the newest. For example, using `os.scandir` and sorting by mod time:

```python
from pathlib import Path

def get_latest_file(dir_path, pattern="*.xlsx"):
    # Get list of files matching pattern and sort by last modified time (newest first)
    files = list(Path(dir_path).glob(pattern))
    if not files:
        return None
    latest = max(files, key=lambda f: f.stat().st_mtime)
    return latest
```

This will return the most recently modified `.xlsx` file in the given directory. (Alternatively, we could sort all entries by `entry.stat().st_mtime` and take the first.) We then use **pandas** to read the Excel file into a DataFrame. Pandas makes it easy to load Excel data: “Read an Excel file into a pandas DataFrame.” For example:

```python
import pandas as pd

latest_serial_file = get_latest_file(SERIAL_NUMBERS_DIR)
latest_delivery_file = get_latest_file(DELIVERY_INFO_DIR)

df_serial = pd.read_excel(latest_serial_file)   # ZMDESNR latest snapshot
df_delivery = pd.read_excel(latest_delivery_file)  # VL06O latest snapshot
```

Pandas handles .xlsx files seamlessly, creating a DataFrame for each. (If the Excel contains multiple sheets, you can specify `sheet_name` as needed, but here we assume the data is on the first sheet or the only sheet.)

## Step 2: Filter by Warehouse and Clean Status Data

Once the data is loaded, we filter the serial-level DataFrame to include only records for the target warehouse:

```python
df_serial = df_serial[df_serial['Warehouse Number'] == WAREHOUSE_FILTER]
```

This ensures we only track activity in warehouse “E01”. Next, we handle the status codes. The `STATUS_MAPPING` given is `{'ASH': 'picked', 'SHP': 'shipped / closed'}`. We apply this mapping to the DataFrame for clarity:

```python
df_serial['StatusText'] = df_serial['Status'].map(STATUS_MAPPING)
```

However, a critical rule is: _if a serial appears as both ASH and SHP, treat it as SHP._ In practice, within a single snapshot each serial number likely appears only once (with its current status). But if we compare snapshots over time, a serial that was “ASH” (picked) may now be “SHP” (shipped). We should avoid counting it twice. Our approach: for the current snapshot, assume it reflects the latest status. If the same serial had an earlier picked status, the current snapshot will show it as shipped, so just using the latest snapshot inherently handles this (the serial will be counted as shipped). If we ever needed to merge information across snapshots, we would drop the older “ASH” entry in favor of “SHP”. In code, one way to enforce this is to drop any “picked” entry for a serial that also has a “shipped” entry in the DataFrame:

```python
# Identify serials that are shipped
shipped_serials = set(df_serial[df_serial['Status'] == 'SHP']['Serial #'])
# Remove any rows where the serial is in shipped_serials but status is ASH
df_serial = df_serial[~((df_serial['Serial #'].isin(shipped_serials)) & (df_serial['Status'] == 'ASH'))]
```

This ensures no serial is counted twice; if it was shipped, we only keep the shipped record.

## Step 3: Aggregate Scanned Packages by User and Delivery

Each row in `df_serial` now represents a scanned serial number (filtered to E01 and cleaned statuses). We need to aggregate this data by user and by delivery to find how many **packages** each user has scanned for each delivery, and how many of those are picked vs shipped.

**Determine “packages” from serials:** The ZMDESNR data has a “Pallet” column, which serves as a package identifier (one pallet corresponds to one physical package). If multiple serials belong to the same pallet, that means those serials are part of one package. To avoid over-counting, we should count unique pallet IDs. For example, if a user scanned 5 serials all under Pallet ID `P12345`, that’s **1 package** scanned (not 5).

We can group the serial data by delivery, user, and pallet. A convenient approach is to aggregate with `.nunique()` on the Pallet column:

```python
agg = df_serial.groupby(['Created by', 'Delivery']).agg(
    scanned_packages=('Pallet', 'nunique')
).reset_index()
```

At this point, `agg` has one row per user & delivery, with the count of unique pallets they scanned (`scanned_packages`). Next, we want the **status breakdown** (how many of those packages are still “picked” vs “shipped / closed”). We can derive this by counting unique pallets per status:

One way is to first reduce the serial DataFrame to unique pallet-level records (since we only need one entry per package). For example, pick the first serial of each pallet or otherwise collapse serials by pallet. We’ll assume that all serials in a pallet share the same status in the snapshot (likely true, as a package would be shipped as a whole). We can do:

```python
# Collapse to one record per pallet (per delivery, per user)
df_pallets = df_serial.groupby(['Created by', 'Delivery', 'Pallet']).agg({
    'StatusText': 'first'   # take the status text of the first serial in that pallet
}).reset_index()
```

Now each row of `df_pallets` is a unique package scanned by a user, with a status of either "picked" or "shipped / closed". We can count statuses easily:

```python
status_counts = df_pallets.groupby(['Created by', 'Delivery', 'StatusText']).size().unstack(fill_value=0)
# Rename columns to 'picked_count' and 'shipped_count'
status_counts.columns = [col.lower() + '_count' for col in status_counts.columns]
status_counts = status_counts.reset_index()
```

The result `status_counts` will have columns `picked_count` and `shipped / closed_count` (the latter name might be cumbersome; we could rename it to `shipped_count` for brevity). We merge this with our `agg` DataFrame on user+delivery:

```python
agg = agg.merge(status_counts, on=['Created by', 'Delivery'], how='left')
agg.fillna(0, inplace=True)  # fill 0 for any missing counts
```

At this stage, `agg` has for each user & delivery: the number of scanned packages, how many of those are picked_count and shipped_count.

Next, we incorporate the **delivery total packages** from the VL06O data. The `df_delivery` DataFrame contains each delivery and a column (as seen in the VL06O screenshot) “Number of packages”. We join this into our aggregate:

```python
df_delivery['Delivery'] = df_delivery['Delivery'].astype(str)
agg['Delivery'] = agg['Delivery'].astype(str)

agg = agg.merge(df_delivery[['Delivery', 'Number of packages']], on='Delivery', how='left')
agg.rename(columns={'Number of packages': 'delivery_total_packages'}, inplace=True)
```

Here we ensure the Delivery keys are strings (to account for leading zeros mismatch). After this, `agg['delivery_total_packages']` gives the total packages in that delivery (for context, e.g. “20” in the example “4 of 20 packages scanned”). The field **“4 of 20 packages scanned”** can be derived from `scanned_packages` and `delivery_total_packages`. The dashboard can display it using those two numbers, or we could format a string, but since we want structured data output, we will keep them separate as numbers.

## Step 4: Calculate Time Metrics per User

We need to compute two time-based metrics for each user: the **last scan time** and the **time between scans**. These are user-specific (not per delivery, since they relate to the user’s activity timeline). We will compute them from the serial scan data. The ZMDESNR data has “Created on” (date) and “Time” columns for each scan. We can combine these into a single datetime timestamp for each serial scan:

```python
df_serial['Timestamp'] = pd.to_datetime(df_serial['Created on'] + ' ' + df_serial['Time'])
```

Now, to get each user’s last scan time, we find the maximum Timestamp per user. Similarly, the time between the last two scans can be found if we get the two latest timestamps per user. One approach: sort by Timestamp and take the last two:

```python
user_times = df_serial.sort_values('Timestamp').groupby('Created by')['Timestamp'].agg(list)
```

This gives a series of timestamp lists per user. For each user’s list, the last element is the last scan time, and the difference between the last and second-last is the previous interval. We can calculate:

```python
user_metrics = []
for user, times in user_times.items():
    if not times:
        continue
    last_time = times[-1]
    prev_time = times[-2] if len(times) > 1 else None
    time_since_last = pd.Timestamp.now() - last_time  # how long since last scan
    time_between = (last_time - prev_time) if prev_time else pd.Timedelta(0)
    user_metrics.append({
        'Created by': user,
        'last_scan_time': last_time,
        'time_since_last_scan': time_since_last,
        'time_between_scans': time_between
    })
user_metrics = pd.DataFrame(user_metrics)
```

The `time_since_last_scan` tells us how long it’s been since the user’s last activity (this could be formatted in minutes or seconds as needed for the dashboard), and `time_between_scans` is the gap between the user’s last two scans (which indicates their scanning pace). For users with only one scan, we set `time_between_scans` to 0 or None. We then merge `user_metrics` into our main `agg` table (on `Created by` = user) so that each user-delivery row in `agg` gets the user’s time metrics. (This will repeat the same `last_scan_time` and intervals on multiple rows for the same user, which is fine for data output.)

Finally, we can rename columns: for clarity, perhaps rename “Created by” to “user” and ensure all columns match the desired output fields (e.g. `picked_count` and `shipped_count` as part of status breakdown).

## Step 5: Output the Aggregated Data (JSON or Parquet)

With the aggregated DataFrame ready, we output it to the `OUT_DIR` in a structured format. The choice is JSON or Parquet. Parquet is a compact binary format that is efficient for large data and retains data types, whereas JSON is human-readable text and convenient for smaller data or direct use in web applications. Given maintainability, we might choose Parquet for efficiency (since Pandas has built-in support), but JSON is equally one-line to write.

We’ll demonstrate using Parquet (assuming either **pyarrow** or **fastparquet** is installed for pandas to use):

```python
# Ensure output directory exists
Path(OUT_DIR).mkdir(exist_ok=True)

# Generate a timestamped filename for output
ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
out_file = Path(OUT_DIR) / f"output_{ts}.parquet"

agg.to_parquet(out_file)
```

Pandas provides a core function `to_parquet()`. We can simply call `df.to_parquet('myfile.parquet')` to save the DataFrame in Parquet format. (If no Parquet engine is installed, you may need to `pip install pyarrow` or similar.)

Alternatively, to output JSON, one could do:

```python
out_file = Path(OUT_DIR) / f"output_{ts}.json"
agg.to_json(out_file, orient='records')
```

Using `orient='records'` will produce a list of dictionaries (one per row) in the JSON file. This is convenient for downstream consumption.

After writing the new output, the tool should manage old files. We keep only the 5 latest files in `OUT_DIR`. We can list the files and delete the oldest ones:

```python
outputs = sorted(Path(OUT_DIR).glob("output_*.parquet"), key=lambda f: f.stat().st_mtime, reverse=True)
for old_file in outputs[5:]:
    old_file.unlink()  # delete files beyond the 5 most recent
```

This sorts output files by modification time (newest first) and then removes everything after the 5th file. (If using JSON, adjust the glob pattern accordingly.)

## Step 6: Scheduling Periodic Updates

To meet the requirement of updating at a set interval (default 60 seconds), we will wrap the above process in a loop or scheduler. A simple approach is an infinite loop with `time.sleep()`. For example:

```python
import time, logging

def main():
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            process_one_cycle()  # call the function that implements Steps 1-5
            logging.info("Snapshot processed and output updated.")
        except Exception as e:
            logging.exception("Error during processing cycle")
        time.sleep(INTERVAL)
```

Using a `while True: ... time.sleep(60)` loop will run the job every 60 seconds. The Redwood scheduling guide shows that this simple loop approach calls the job and then sleeps for the specified interval. We’ve added a `try/except` so that any runtime error in processing is caught and logged – the loop will continue rather than crash on one bad cycle. The `logging.basicConfig` is used to configure logging output (here we log to console with INFO level; we could also log to a file by specifying a filename in `basicConfig`). Logging each cycle’s success or failure provides basic monitoring.

> **Logging Example:** By configuring `logging` (e.g., `logging.basicConfig(filename='process.log', level=logging.INFO)`), we can record events. For instance, calling `logging.info()` or `logging.error()` will output messages with timestamps. The Python logging HOWTO demonstrates writing logs to a file with different severity levels. In our script, we use `logging.exception()` inside the exception handler, which logs the error with traceback automatically.

## Implementation: Modular Script Outline

Bringing it all together, below is a structured outline of the Python script. This is organized into functions for clarity:

- `get_latest_file(dir_path)`: returns the newest snapshot file path from a directory.
- `process_snapshot()`: performs one full cycle of reading files, processing data, and writing output.
- `main()`: runs `process_snapshot` in a loop with the configured interval.

```python
import os, time, logging
from pathlib import Path
import pandas as pd
import config  # import the constants from config.py

def get_latest_file(directory, pattern="*.xlsx"):
    files = list(Path(directory).glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)

def process_snapshot():
    # 1. Get latest files
    serial_file = get_latest_file(config.SERIAL_NUMBERS_DIR, "*.xlsx")
    delivery_file = get_latest_file(config.DELIVERY_INFO_DIR, "*.xlsx")
    if not serial_file or not delivery_file:
        logging.warning("No files found to process (serial_file=%s, delivery_file=%s)", serial_file, delivery_file)
        return

    # 2. Read Excel files
    df_serial = pd.read_excel(serial_file)
    df_delivery = pd.read_excel(delivery_file)
    logging.info(f"Loaded {serial_file.name} and {delivery_file.name}")

    # 3. Filter warehouse
    df_serial = df_serial[df_serial['Warehouse Number'] == config.WAREHOUSE_FILTER]
    # Map status codes to text
    df_serial['StatusText'] = df_serial['Status'].map(config.STATUS_MAPPING)
    # Treat ASH+SHP as SHP (remove picked if same serial is shipped)
    shipped_serials = set(df_serial[df_serial['Status'] == 'SHP']['Serial #'])
    df_serial = df_serial[~((df_serial['Serial #'].isin(shipped_serials)) & (df_serial['Status'] == 'ASH'))]

    # 4. Aggregate by user & delivery (unique pallets and status counts)
    df_pallets = df_serial.groupby(['Created by', 'Delivery', 'Pallet']).agg({
        'StatusText': 'first'
    }).reset_index()
    # Count scanned packages (unique pallets per user-delivery)
    agg = df_pallets.groupby(['Created by', 'Delivery']).agg(
        scanned_packages=('Pallet', 'count')
    ).reset_index()
    # Status counts
    status_counts = df_pallets.groupby(['Created by', 'Delivery', 'StatusText']).size().unstack(fill_value=0)
    status_counts = status_counts.rename(columns={'picked': 'picked_count', 'shipped / closed': 'shipped_count'}).reset_index()
    agg = agg.merge(status_counts, on=['Created by','Delivery'], how='left').fillna(0)

    # 5. Add total delivery packages from VL06O
    df_delivery['Delivery'] = df_delivery['Delivery'].astype(str)
    agg['Delivery'] = agg['Delivery'].astype(str)
    agg = agg.merge(df_delivery[['Delivery', 'Number of packages']], on='Delivery', how='left')
    agg = agg.rename(columns={'Number of packages': 'delivery_total_packages'})

    # 6. Compute time metrics per user
    df_serial['Timestamp'] = pd.to_datetime(df_serial['Created on'] + ' ' + df_serial['Time'])
    user_last = df_serial.groupby('Created by')['Timestamp'].agg(['max', 'nlargest'])
    # 'max' gives last, 'nlargest' gives a Series of top values
    user_last = df_serial.sort_values('Timestamp').groupby('Created by')['Timestamp'].agg(list)
    user_metrics_list = []
    for user, timestamps in user_last.items():
        if not timestamps:
            continue
        last_time = timestamps[-1]
        prev_time = timestamps[-2] if len(timestamps) > 1 else None
        time_since = pd.Timestamp.now() - last_time
        time_between = last_time - prev_time if prev_time else pd.Timedelta(0)
        user_metrics_list.append({
            'Created by': user,
            'last_scan_time': last_time,
            'time_since_last_scan': time_since,
            'time_between_scans': time_between
        })
    user_metrics = pd.DataFrame(user_metrics_list)
    agg = agg.merge(user_metrics, on='Created by', how='left')

    # 7. Prepare output DataFrame
    agg = agg.rename(columns={'Created by': 'user', 'Delivery': 'delivery'})
    # (reorder columns if desired)

    # 8. Compare with previous output to detect changes
    latest_out = get_latest_file(config.OUT_DIR, "*.parquet")
    changed = True
    if latest_out:
        try:
            prev_df = pd.read_parquet(latest_out)
            # Compare new vs old
            # (Sort by user,delivery for stable comparison)
            cols = agg.columns
            prev_df = prev_df[cols].sort_values(['user','delivery']).reset_index(drop=True)
            new_df = agg[cols].sort_values(['user','delivery']).reset_index(drop=True)
            if new_df.equals(prev_df):
                changed = False
        except Exception as e:
            logging.error("Could not read previous output: %s", e)

    if not changed:
        logging.info("No changes detected in data. Output not updated.")
        return

    # 9. Write new output to Parquet (or JSON)
    Path(config.OUT_DIR).mkdir(exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(config.OUT_DIR) / f"output_{timestamp}.parquet"
    agg.to_parquet(out_path)
    logging.info(f"Wrote output file: {out_path.name}")

    # 10. Cleanup old outputs (keep last 5)
    outputs = sorted(Path(config.OUT_DIR).glob("*.parquet"), key=lambda f: f.stat().st_mtime, reverse=True)
    for old_file in outputs[5:]:
        try:
            old_file.unlink()
            logging.info(f"Removed old output file: {old_file.name}")
        except Exception as e:
            logging.warning(f"Failed to remove {old_file.name}: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting shipment tracking service...")
    while True:
        try:
            process_snapshot()
        except Exception:
            logging.exception("Unexpected error in processing cycle")
        time.sleep(config.INTERVAL)

if __name__ == "__main__":
    main()
```

**Explanation:** The `process_snapshot()` function encapsulates one polling cycle. It loads the latest files, processes the data as described in steps 2–6, and writes out a new Parquet snapshot if changes are detected. We use logging at each significant step (file loaded, output written, errors, etc.). The `main()` function configures logging and runs `process_snapshot()` in an infinite loop, sleeping for `config.INTERVAL` (60 seconds) between runs. This loop will continuously update the output data periodically.

## Conclusion

This Python tool will continuously monitor the specified directories for new Excel snapshots and update a consolidated user-by-delivery scan status report. By aggregating on unique pallet IDs, it accurately counts packages scanned, and by cross-referencing with the total packages per delivery, it can report progress like “X of Y packages scanned.” The inclusion of timestamps allows tracking user activity (e.g., highlighting if a user has been idle since a certain time). The output, written in a convenient format (Parquet or JSON), can be picked up by a dashboard application to display real-time status of picking and shipping operations.

**Logging and Error Handling:** We included logging statements for normal operations (`INFO` level) and used `logging.exception` to capture stack traces for any errors, which will greatly aid in debugging if something goes wrong during the periodic runs. Basic checks (like verifying files exist before processing) help avoid crashes.

With this setup, the tool is modular and maintainable: configuration is isolated in `config.py`, and the script can be run as a service (for example, using a persistent Python process or as a Windows service or cron job). The core logic uses idiomatic pandas operations for data manipulation and is designed to be efficient by only processing incremental changes (latest files and change detection against last output). The result is a robust automation that meets the stated requirements.
