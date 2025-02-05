

import os
import pandas as pd
from sqlalchemy import create_engine

# Database connection configuration
DATABASE_URI = 'postgresql://postgres:Sup%4072164@localhost:5432/Taxanomy'

log_folder_path = r'D:\FinancialStatementAnalysis\01ETL\04logs'  # Change to the desired root path for logs


# Function to validate and add missing columns
def validate_columns(final_data, required_columns):
    missing_columns = [col for col in required_columns if col not in final_data.columns]
    if missing_columns:
        print(f"Adding missing columns: {missing_columns}")
        for col in missing_columns:
            final_data[col] = None  # Add missing columns with default None values
    return final_data

# Function to load the master mapping from the input table in PostgreSQL
def load_master_mapping():
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URI)

        query = """
        SELECT
            "Unit-Element_Name",
            "Taxonomy_id"
        FROM taxonomy_table;
        """  # Column names remain as-is

        # Load master mapping into a DataFrame
        master_mapping = pd.read_sql(query, engine)
        print("Master mapping loaded successfully.")
        return master_mapping
    except Exception as e:
        print(f"Error loading master mapping: {e}")
        exit()

# Function to save the final data to the output table in PostgreSQL
def save_to_postgres(final_data, table_name):
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URI)

        # Validate columns before saving
        required_columns = [
            "Taxonomy_id", "Company Code", "Financial Year", "Quarter",
            "Element Name", "Unit", "Value", "Decimal", "Unit-Element_Name", "Period Start Date", "Period End Date"
        ]
        final_data = validate_columns(final_data, required_columns)

        # Reorder columns to make sure Taxonomy_Id is the first column
        column_order = ["Taxonomy_id", "Company Code", "Financial Year", "Quarter",
                        "Element Name", "Unit", "Value", "Decimal", "Unit-Element_Name", "Period Start Date", "Period End Date"]
        final_data = final_data[column_order]  # Reorder the columns as per the specified order

        # Save data to PostgreSQL
        print(f"Attempting to save data to table '{table_name}'...")
        final_data.to_sql(table_name, engine, index=False, if_exists='append')
        print(f"Data successfully saved to table '{table_name}'.")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")

def process_all_companies(root_folder, master_mapping, converted_folder, log_format='csv'):
    final_data = pd.DataFrame()
    missing_taxonomy_log = []  # List to store the log entries only for missing taxonomy IDs

    # To track folder serial numbers
    folder_serials_with_data = []

    for company_folder_name in os.listdir(root_folder):
        company_folder_path = os.path.join(root_folder, company_folder_name)
        if os.path.isdir(company_folder_path):  # Ensure it's a folder
            print(f"Processing folder for company: {company_folder_name}")

            # Extract the serial number from the folder name
            folder_serial = company_folder_name.split('_')[0] if '_' in company_folder_name else None
            if not folder_serial or not folder_serial.isdigit():
                print(f"Skipping folder with invalid serial number format: {company_folder_name}")
                continue

            # Process the company folder
            company_data = process_company_folder(company_folder_path)

            if not company_data.empty:
                # Add the folder serial to the list (only if data exists)
                folder_serials_with_data.append(int(folder_serial))

                # Merge with the master mapping
                merged_data = pd.merge(
                    company_data,
                    master_mapping,
                    on='Unit-Element_Name',
                    how='left'
                )

                # Log only missing taxonomy IDs (where Taxonomy_id is NaN)
                missing_taxonomy = merged_data[merged_data['Taxonomy_id'].isna()]
                if not missing_taxonomy.empty:
                    # Add log entries for missing taxonomy (where Taxonomy_id is NaN)
                    for unit_element_name in missing_taxonomy['Unit-Element_Name']:
                        missing_taxonomy_log.append({
                            'Company': company_folder_name,
                            'Unit-Element_Name': unit_element_name  # Only log the missing info
                        })

                # Drop the 'Status' column only if it exists
                if 'Status' in merged_data.columns:
                    merged_data = merged_data.drop(columns=['Status'])

                final_data = pd.concat([final_data, merged_data], ignore_index=True)

                # Move processed files to the converted folder with 'loaded' instead of 'Converted_Excels'
                processed_folder_path = os.path.join(converted_folder, f"{company_folder_name.replace('Converted_Excels', 'loaded')}")
                os.makedirs(processed_folder_path, exist_ok=True)

                for file_name in os.listdir(company_folder_path):
                    file_path = os.path.join(company_folder_path, file_name)
                    if os.path.isfile(file_path):
                        new_file_path = os.path.join(processed_folder_path, file_name)
                        os.rename(file_path, new_file_path)
                        print(f"Moved {file_name} to {processed_folder_path}.")

    # Save the missing taxonomy log to the desired format
    log_df = pd.DataFrame(missing_taxonomy_log)
    if folder_serials_with_data:
        # Determine the starting and ending serial numbers
        start_serial = min(folder_serials_with_data)
        end_serial = max(folder_serials_with_data)

        # Construct the log file name
        log_file_name = f"{start_serial}_to_{end_serial}_missing_taxonomy"

        os.makedirs(log_folder_path, exist_ok=True)
         # Save the log file in the defined log folder
        log_file_path = os.path.join(log_folder_path, f"{log_file_name}.{log_format}")

        # Save as CSV or Excel based on the chosen format
        if log_format == 'csv':
            log_df.to_csv(f"{log_file_path }.csv", index=False)
            print(f"Log file saved as {log_file_path }.csv.")
        elif log_format == 'xlsx':
            log_df.to_excel(f"{log_file_path }.xlsx", index=False, engine='openpyxl')  # Explicitly use 'openpyxl'
            print(f"Log file saved as {log_file_path }.xlsx.")
        else:
            print("Invalid log format. Please choose 'csv' or 'xlsx'.")
    else:
        print("No folders with data were found; no log file created.")

    return final_data

# Function to process a single Excel file
def process_excel(file_path):
    try:
        if file_path.endswith(('.xlsx', '.xls')):
            print(f"Processing {file_path}")
            df = pd.read_excel(file_path, engine='openpyxl')

            # Check for ScripCode, Symbol, or ISIN in Element Name to find the relevant starting index
            relevant_keywords = ['ScripCode', 'Symbol', 'ISIN']
            relevant_row_index = None
            for idx, row in df.iterrows():
                if any(keyword in str(row['Element Name']) for keyword in relevant_keywords):
                    relevant_row_index = idx
                    break

            # Trim the data starting from this row
            if relevant_row_index is not None:
                df = df.iloc[relevant_row_index:]

            # Clean the data
            df = df[~df['Value'].isin(['Unknown', ''])]  # Remove rows with 'Unknown' or empty 'Value'
            df = df[df['Element Name'].notna()]  # Remove rows with empty 'Element Name'

            # Create 'Unit-Element_Name' column
            df['Unit-Element_Name'] = df['Unit'] + "-" + df['Element Name']

            # Reorder columns to match the final output
            df = df[['Company Code', 'Financial Year', 'Quarter', 'Element Name', 'Unit', 'Value', 'Decimal', 'Unit-Element_Name', 'Period Start Date', 'Period End Date']]

            return df
        else:
            print(f"Skipping non-Excel file: {file_path}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error with file {file_path}: {e}")
        return pd.DataFrame()

# Function to process all files in a company folder
def process_company_folder(company_folder_path):
    all_data = pd.DataFrame()
    for file_name in os.listdir(company_folder_path):
        file_path = os.path.join(company_folder_path, file_name)
        if os.path.isfile(file_path):
            file_data = process_excel(file_path)
            if not file_data.empty:
                all_data = pd.concat([all_data, file_data], ignore_index=True)
    return all_data

# Main function
def main():
    root_folder_path = r'D:\FinancialStatementAnalysis\01ETL\Transform'  # Root folder containing company folders
    converted_folder_path = r'D:\FinancialStatementAnalysis\01ETL\load'  # Folder to store converted Excel files
    output_table_name = 'taxonomy_output'  # Correct output table name

    log_format = 'csv'  # Set the log format here (either 'csv' or 'xlsx')
   
    print("Starting data processing...")

    # Load master mapping from PostgreSQL
    master_mapping = load_master_mapping()

    # Process all company folders and merge data
    final_data = process_all_companies(root_folder_path, master_mapping, converted_folder_path, log_format=log_format)

    # Save the merged data to the correct output table
    if not final_data.empty:
        save_to_postgres(final_data, output_table_name)
        print("Data processing completed successfully!")
    else:
        print("No data was processed or extracted.")

if __name__ == "__main__":
    main()