import os
import time
import pandas as pd
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from openpyxl import Workbook

# Chrome options
options = Options()
options.add_argument("--start-maximized")
options.add_argument("--headless")  # Uncomment if you don't need to see browser interactions

# Initialize a list to hold log data
log_data = []

def log_message(stock_name, file_name, url, status, error_line=None):
    log_data.append({
        "Stock Name": stock_name,
        "File Name": file_name,
        "URL": url,
        "Status": status,
        "Error Line": error_line
    })

# Function to scrape the page content and save it to Excel using Selenium
def scrape_page_content_selenium(driver, excel_file_path):
    try:
        # Find the table with the class 'largetable'
        table = driver.find_element(By.CLASS_NAME, 'largetable')

        # Extract table rows
        rows = table.find_elements(By.TAG_NAME, 'tr')
        
        # Prepare a list to store the rows data
        data = []
        
        for row in rows:
            # Extract columns (both 'th' and 'td')
            cols = row.find_elements(By.TAG_NAME, 'td')  # You can also include 'th' if there are headers
            cols_text = [col.text.strip() for col in cols]
            data.append(cols_text)

        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame(data)
        
        # Save the DataFrame to an Excel file
        df.to_excel(excel_file_path, index=False, header=False)  # header=False if there's no header row
        print(f"Successfully scraped and saved the page content to: {excel_file_path}")
    
    except Exception as e:
        print(f"Error scraping page content: {str(e)}")

# XML extraction logic including scraping the page
def XML_extraction(row_number, Security_code, Stock_Name, Save_Folder):
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"
    driver = webdriver.Chrome(options=options)
    driver.get(Top_URL)
    print(Stock_Name)

    try:
        # Search for the security code
        Security_Search = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_SmartSearch_smartSearch")))
        Security_Search.clear()
        Security_Search.send_keys(Security_code)

        # Select the correct company from the dropdown
        li_element = driver.find_element(By.XPATH, f"//li[contains(@onclick, \"'{Security_code}'\")]")
        li_element.click()

        # Select "Quarterly" from the dropdown
        dropdown = driver.find_element(By.ID, "ContentPlaceHolder1_broadcastdd")
        select = Select(dropdown)
        select.select_by_value("7")

        # Click the submit button
        Submit_button = driver.find_element(By.ID, "ContentPlaceHolder1_btnSubmit")
        Submit_button.click()

        # Find all the links for downloading files
        rows = driver.find_elements(By.XPATH, f"//td[text()='{Security_code}']/following-sibling::td[3]//a")
        File_Name_rows = driver.find_elements(By.XPATH, f"//td[text()='{Security_code}']/following-sibling::td[3]//a")
        time.sleep(1)

        for i in range(len(rows)):
            link = rows[i]
            File_Name = File_Name_rows[i].text
            print(File_Name)

            # Scroll to the link before clicking
            driver.execute_script("arguments[0].scrollIntoView(true);", link)
            time.sleep(1)

            # Click the link to open the file in a new tab
            link.click()
            driver.switch_to.window(driver.window_handles[-1])
            current_url = driver.current_url
            print(current_url)

            # Define the Excel file path
            custom_excel_file_name = f"{Stock_Name}_{File_Name}.xlsx"  # Excel file name
            custom_excel_file_path = os.path.join(Save_Folder, custom_excel_file_name)  # Excel path

            # Scrape the page content into the Excel file
            scrape_page_content_selenium(driver, custom_excel_file_path)

            # Log success
            log_message(Stock_Name, File_Name, current_url, "Success")

            # Close the tab and return to the original window
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)

    except Exception as e:
        tb_str = traceback.format_exc()
        error_line = 'Unknown'
        for line in tb_str.splitlines():
            if 'File' in line and ', line ' in line:
                error_line = line.strip()
                break
        log_message(Stock_Name, "N/A", Top_URL, "Extraction Failed", error_line)
        print(f"Error occurred during XML extraction for {Stock_Name}: {str(e)}")

    finally:
        driver.quit()

# Path to your input Excel file
Sample_List = r"D:\lifeinsurance_excel\input\sampleinput_life.xlsx"

# Read the Excel file
df = pd.read_excel(Sample_List, sheet_name='Sheet1')

# Ask the user for the range of rows they want to iterate over
start_row = int(input("Enter the start row number (e.g., 10): "))
end_row = int(input("Enter the end row number (e.g., 20): "))

# Validate the row range
if start_row < 1 or end_row > len(df):
    print(f"Invalid range. Please enter a range between 1 and {len(df)}")
else:
    df_range = df.iloc[start_row-1:end_row]

    # Base path for saving XML files
    base_path = r"D:\lifeinsurance_excel\excel"
   
    # Using the Excel row number for saving files
    for row_number, (index, row) in enumerate(df_range.iterrows(), start=start_row):
        Security_code = str(row['Security Code'])
        Stock_Name = str(row['Symbol'])
        Sr_No = str(row['Sr. No.'])  # Extract the 'Sr.No.' column from the DataFrame

        # Create the folder with only Sr.No. and company name
        folder_name = f"{Sr_No}_{Stock_Name}"  # Use only Sr.No. and stock name for folder name
        Save_Folder = os.path.join(base_path, folder_name)
        os.makedirs(Save_Folder, exist_ok=True)

        # Pass the row number from Excel to the XML extraction function
        XML_extraction(row_number, Security_code, Stock_Name, Save_Folder)

    # Save the log data to an Excel file
    base_log_path = r"D:\lifeinsurance_excel\log"
    os.makedirs(base_log_path, exist_ok=True)

    log_file_name = f"log_rows_{start_row}_to_{end_row}.xlsx"
    log_df = pd.DataFrame(log_data, columns=["Stock Name", "File Name", "URL", "Status", "Error Line"])
    log_df.to_excel(os.path.join(base_log_path, log_file_name), index=False)
    print("Process complete")
