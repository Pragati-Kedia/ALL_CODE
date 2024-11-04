import os
from turtle import pd
import pandas as pd
import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
 
# Chrome options
options = Options()
options.add_argument("--start-maximized")
options.add_argument("--headless")  # If you want to see browser interactions, comment this line
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
 
def XML_extraction(driver, stock_name, save_folder):
    try:
        # Find all XML links on the page
        rows = driver.find_elements(By.XPATH, "//td[6]//a")  # Adjust based on actual table structure
        time.sleep(1)
 
        for i, link in enumerate(rows):
            File_Name = link.text  # Use the link text for the file name
            print(f"Processing: {File_Name}")
            link.click()
            driver.switch_to.window(driver.window_handles[-1])
            current_url = driver.current_url
            print(f"Current URL: {current_url}")
 
            # Define the custom file name using stock_name, File_Name, and index
            custom_file_name = f"{stock_name}_{i + 1}.xml"  # Append index for uniqueness
            custom_file_path = os.path.join(save_folder, custom_file_name)
 
            try:
                # Extract the XML content directly
                xml_div = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'webkit-xml-viewer-source-xml')))
                xml_content = xml_div.get_attribute('innerHTML')  # Extract the XML content
 
                # Save XML content
                with open(custom_file_path, 'w', encoding='utf-8') as file:
                    file.write(xml_content)
 
                # Log success
                log_message(stock_name, File_Name, current_url, "Success")
 
            except Exception as e:
                tb_str = traceback.format_exc()
                error_line = 'Unknown'
                for line in tb_str.splitlines():
                    if 'File' in line and ', line ' in line:
                        error_line = line.strip()
                        break
                log_message(stock_name, File_Name, current_url, "File not saved", error_line)
                print(f"Error saving XML file for {stock_name} - {File_Name}: {str(e)}")
 
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
        log_message(stock_name, "N/A", "N/A", "Extraction Failed", error_line)
        print(f"Error occurred during XML extraction for {stock_name}: {str(e)}")
 
 
def main():
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"
    driver = webdriver.Chrome(options=options)
    driver.get(Top_URL)
 
    # Create a base path for saving XML files
    base_path = r"D:\Consolidated_xml_file\xml"
    os.makedirs(base_path, exist_ok=True)
 
    try:
        # You can set a fixed stock name or generate one based on the current URL
        stock_name = "Random_Company"  # Placeholder name, adjust as needed
 
        # Create a folder for saving files
        Save_Folder = os.path.join(base_path, stock_name)
        os.makedirs(Save_Folder, exist_ok=True)
 
        # Start the XML extraction process
        XML_extraction(driver, stock_name, Save_Folder)
 
    finally:
        driver.quit()
 
    # Save the log data to an Excel file
    base_log_path = r"D:\Consolidated_xml_file\log"
    os.makedirs(base_log_path, exist_ok=True)
 
    log_file_name = "log.xlsx"
    log_df = pd.DataFrame(log_data, columns=["Stock Name", "File Name", "URL", "Status", "Error Line"])
    log_df.to_excel(os.path.join(base_log_path, log_file_name), index=False)
 
    print("Process complete")
 
if __name__ == "__main__":
    main()
