from lxml import etree
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil
import traceback
from datetime import datetime
 
log_columns = ['Stock', 'Period', 'Status', 'Message', 'Error Line']
log_df = pd.DataFrame(columns=log_columns)
 
def load_xml_lxml(file_path):
    """Load and parse the XML file using lxml."""
    try:
        tree = etree.parse(file_path)
        root = tree.getroot()
        print(f"Root Element: {root.tag}")  # Debugging line
        return root
    except etree.XMLSyntaxError as e:
        print(f"Error parsing XML file: {e}")
        raise
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        raise
 
def extract_scrip_code_from_context(root):
    """Extract Scrip Code based on the Element Name 'ScripCode'."""
    scrip_code_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "ScripCode"), None)
    return scrip_code_row.text.strip() if scrip_code_row is not None else 'Unknown'
 
def extract_financial_year_from_context(root):
    """Extract Financial Year based on the Element Name 'DateOfEndOfFinancialYear'."""
    financial_year_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfFinancialYear"), None)
    if financial_year_row is not None:
        date_value = financial_year_row.text.strip()
        year = datetime.strptime(date_value, "%Y-%m-%d").year
        return f"{year}"
    return 'Unknown'
 
def extract_quarter_from_context(root):
    """Extract Quarter based on the Element Name 'DateOfStartOfReportingPeriod'."""
    start_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfStartOfReportingPeriod"), None)
    if start_date_row is not None:
        date_value = start_date_row.text.strip()
        month = datetime.strptime(date_value, "%Y-%m-%d").month
        if 1 <= month <= 3:
            return '04'
        elif 4 <= month <= 6:
            return '01'
        elif 7 <= month <= 9:
            return '02'
        elif 10 <= month <= 12:
            return '03'
    return 'Unknown'
 
def extract_all_data(root):
    """Extract all data from the XML, including the new columns."""
    all_data = []
   
    # Extract existing data (financial year, quarter)
    scrip_code = extract_scrip_code_from_context(root)
    financial_year = extract_financial_year_from_context(root)
    quarter = extract_quarter_from_context(root)
 
    # Extract Period Start and Period End Dates from the XML
 
    period_start_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfStartOfReportingPeriod"), None)
    period_end_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfReportingPeriod"), None)
 
    # Set Period Start and Period End Dates, default to 'Unknown' if not found
    period_start_date = period_start_date_row.text.strip() if period_start_date_row is not None else 'Unknown'
    period_end_date = period_end_date_row.text.strip() if period_end_date_row is not None else 'Unknown'
 
    # Iterate over all elements in the XML and extract necessary data
    for elem in root.iter():
        tag = etree.QName(elem).localname  # Handle namespace if present
        value = elem.text.strip() if elem.text else None
        context_ref = elem.get('contextRef', 'OneD')  # Default to 'OneD' if contextRef is missing
        decimals = elem.get('decimals', '')  # Include Decimals if present
        fact_value = value
 
        all_data.append({
            'Company Code': scrip_code,
            'Financial Year': financial_year,
            'Quarter': quarter,
            'Element Name': tag,
            'Unit': context_ref,
            'Value': fact_value,  # Include Decimals
            'Decimal': decimals,  # Rename ContextRef to Unit
            'Period Start Date': period_start_date,  # Add Period Start Date
            'Period End Date': period_end_date,      # Add Period End Date
        })
   
    return all_data
 
 
 
def convert_to_dataframe(data):
    """Convert extracted data to a pandas DataFrame."""
    return pd.DataFrame(data)
 
def XML_edit(filepath):
    """Edit XML to remove the specific comment line."""
    file_path = filepath
    print(file_path)
    time.sleep(2)
    with open(file_path, 'r', encoding='utf-8') as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    for elem in root.iter():
        if elem.tag == ET.Comment and elem.text.strip() == 'FRIndAs':
            root.remove(elem)
    tree = ET.ElementTree(root)
    tree.write(file_path)
    return file_path
 
def process_xml_files(xml_download_dir, excel_save_dir, Processed_XMLs_folder, Stock_Symbol):
    """Process all XML files in the XML download directory and save them to Excel."""
    global log_df
    for root_dir, _, files in os.walk(xml_download_dir):
        for file_name in files:
            if file_name.endswith(".xml"):
                file_path = os.path.join(root_dir, file_name)
                print(f"Processing file: {file_path}")
                try:
                    revised_file_path = XML_edit(file_path)  # Edit the XML file if needed
                    root = load_xml_lxml(revised_file_path)
                    all_data = extract_all_data(root)
                    all_data_df = convert_to_dataframe(all_data)
 
                    # Extract Period Start Date and Period End Date for Excel file naming
                    period_start_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfStartOfReportingPeriod")]
                    period_end_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfEndOfReportingPeriod")]
 
                    period_start_date = period_start_date_row["Value"].values[0] if not period_start_date_row.empty else 'UNKNOWN_START_DATE'
                    period_end_date = period_end_date_row["Value"].values[0] if not period_end_date_row.empty else 'UNKNOWN_END_DATE'
 
                    # Generate the Excel file name based on the Period Start Date or default to UNKNOWN
                    reporting_period_str = f"{period_start_date}_{period_end_date}"
                    base_file_name = file_name.replace(".xml", "")
                    new_file_name = f"{reporting_period_str}_{base_file_name}.xlsx"
 
                    # Write the data to Excel
                    excel_path = os.path.join(excel_save_dir, new_file_name)
                    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                        all_data_df.to_excel(writer, sheet_name='All Data', index=False)
                    print(f"Saved Excel file: {excel_path}")
 
                    # Move the processed XML to another folder
                    destination_xml = os.path.join(Processed_XMLs_folder, file_name)
                    shutil.move(file_path, destination_xml)
 
                    # Log success
                    log_entry = pd.DataFrame([{'Stock': str(Stock_Symbol), 'Period': str(file_name), 'Status': 'Success', 'Message': 'Processing completed successfully.', 'Error Line': None}])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)
 
                except Exception as e:
                    tb_str = traceback.format_exc()
                    error_line = 'Unknown'
                    for line in tb_str.splitlines():
                        if 'File' in line and ', line ' in line:
                            error_line = line.strip()
                            break
                    log_entry = pd.DataFrame([{'Stock': str(Stock_Symbol), 'Period': str(file_name), 'Status': 'Error', 'Message': str(e), 'Error Line': error_line}])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)
 
def replace_year_quarter_prefix(file_name, new_prefix):
    """Replace the 'YYYY-YYYY+1_QN' prefix in the file name with the new 'YYYYMM' prefix."""
    import re
    pattern = r'^\d{4}-\d{4}_Q\d_'
    return re.sub(pattern, f"{new_prefix}_", file_name)

# Define paths
Input_Folder_path = Path(r"D:\FinancialStatementAnalysis\01ETL\extracted")
Output_Folder_Path = Path(r"D:\FinancialStatementAnalysis\01ETL\Transform")
xml_folder_path = Path(r"D:\FinancialStatementAnalysis\01ETL\extracted\XMLS_Processed")
Input_File = (r"D:\FinancialStatementAnalysis\03input\Taxonomy_LOS_61_to_100.xlsx")  
Log_Folder_Path = Path(r"D:\FinancialStatementAnalysis\04log")  # Log folder path
 
# Create the log folder if it does not exist
os.makedirs(Log_Folder_Path, exist_ok=True)
 
# Read the Excel file
df = pd.read_excel(Input_File, sheet_name='Sheet1')
 
# Iterate over each stock and serial number from the input file
for index, row in df.iterrows():
    serial_number = str(row['Sr No'])  # Assuming column name for serial number is 'SerialNumber'
    Name = str(row['Symbol'])
    folder_name = f"{serial_number}_{Name}"  # Combine serial number and stock symbol
    print(f"Looking for folder: {folder_name}")
 
    for current_folder in Input_Folder_path.iterdir():
        if folder_name == str(os.path.basename(current_folder)):
            current_folder_path = os.path.join(Input_Folder_path, current_folder)
            print(f"Processing folder: {current_folder_path}")
            xml_directory = current_folder_path
            Processed_XMLs_folder_name = folder_name + "_XMLS_Processed"
            Processed_XMLs_folder = os.path.join(xml_folder_path, Processed_XMLs_folder_name)
            Converted_Excels_folder_name = folder_name + "_Converted_Excels"
            Converted_Excels_folder = os.path.join(Output_Folder_Path, Converted_Excels_folder_name)
            os.makedirs(Processed_XMLs_folder, exist_ok=True)
            os.makedirs(Converted_Excels_folder, exist_ok=True)
            process_xml_files(xml_directory, Converted_Excels_folder, Processed_XMLs_folder, current_folder)
 
# Save the log data to an Excel file in the log folder
log_file_name = "xml_to_excel_61_to_100.xlsx"
log_file_path = os.path.join(Log_Folder_Path, log_file_name)
log_df.to_excel(log_file_path, index=False)
print('Process complete. Log file saved to:', log_file_path)
