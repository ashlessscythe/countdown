"""
Analyze Excel files in the sample directory to understand their structure.
"""
import os
import pandas as pd
import json
from datetime import datetime

def analyze_excel_file(file_path):
    """Analyze an Excel file and return its structure and sample data"""
    print(f"Analyzing file: {file_path}")
    
    # Read the Excel file
    df = pd.read_excel(file_path, dtype=str)
    
    # Get basic information
    info = {
        "filename": os.path.basename(file_path),
        "rows": len(df),
        "columns": list(df.columns),
        "sample_data": df.head(5).to_dict(orient="records")
    }
    
    return info

def main():
    """Main function to analyze all Excel files in the sample directory"""
    sample_dir = "sample_files"
    results = []
    
    # Get all Excel files
    excel_files = [f for f in os.listdir(sample_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        print("No Excel files found")
        return
    
    print(f"Found {len(excel_files)} Excel files")
    
    # Analyze each Excel file
    for excel_file in excel_files:
        file_path = os.path.join(sample_dir, excel_file)
        info = analyze_excel_file(file_path)
        results.append(info)
    
    # Save the results to a JSON file
    output_file = "excel_analysis.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Analysis saved to: {output_file}")
    
    # Print a summary
    for info in results:
        print(f"\nFile: {info['filename']}")
        print(f"Rows: {info['rows']}")
        print(f"Columns: {', '.join(info['columns'])}")

if __name__ == "__main__":
    main()
