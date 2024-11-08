import os
import time
import shutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from consolidated_xml_to_excel import process_xml_files  # Import the function to convert XML to Excel

# Define the folder path where you want to save the XML files (already existing folder)
save_folder = r"D:\Consolidated_xml_file\xml"  # Folder where XML files will be saved

# Load the Excel Sheet for Security Codes and Symbols
file_path = r"D:\Consolidated_xml_file\Samples500_v2.xlsx"  # Path to your Excel file
df = pd.read_excel(file_path)

def save_xml(symbol, period_value, xml_content, serial):
    """
    Save the extracted XML content to a file and move it to the corresponding symbol folder.
    """
    # Generate the filename for the XML file using Symbol and Period
    xml_filename = os.path.join(save_folder, f"{symbol}_{period_value}.xml")

    # Save the XML content to the file
    with open(xml_filename, "w", encoding="utf-8") as file:
        file.write(xml_content)

    print(f"XML data for {symbol} saved as {xml_filename}")

    # Construct folder names
    symbol_folder_name = f"{serial}_{symbol}_XMLS_Processed"
    excel_folder_name = f"{serial}_{symbol}_Converted_Excels"
    
    # Define the paths for these folders
    symbol_folder_path = os.path.join(r"D:\Consolidated_xml_file\exceloutput", symbol_folder_name)
    excel_folder_path = os.path.join(r"D:\Consolidated_xml_file\exceloutput", excel_folder_name)

    # Check if both folders exist before moving and processing
    if os.path.isdir(symbol_folder_path) and os.path.isdir(excel_folder_path):
        # Move the XML file to the appropriate symbol folder
        shutil.move(xml_filename, symbol_folder_path)
        print(f"Moved XML file to {symbol_folder_path}")

        # Process the XML file into Excel and save it in the Converted_Excels folder
        try:
            process_xml_files(
                xml_download_dir=symbol_folder_path,  # Using symbol folder as download dir
                excel_save_dir=excel_folder_path,     # Save the Excel file in the Converted_Excels folder
                Processed_XMLs_folder=symbol_folder_path,  # Processed XMLs folder
                Stock_Symbol=symbol
            )
            print(f"Converted XML to Excel and saved in {excel_folder_path}")
        except Exception as e:
            print(f"Error processing XML to Excel for {symbol}: {e}")
    else:
        print(f"Skipping {symbol}: Required folders do not exist for XML or Excel.")


def XML_extraction(driver):
    """
    Extract XML data from the website, match it with the symbol from the Excel sheet, and save it.
    """
    try:
        # Find all rows in the table
        rows = driver.find_elements(By.XPATH, "//*[@id='ContentPlaceHolder1_gvData']/tbody/tr")  # Get all rows in the table
        time.sleep(1)

        for i, row in enumerate(rows):
            try:
                # Get the security code and period for matching
                security_code = row.find_element(By.XPATH, ".//td[1]").text.strip()  # Get the Security Code (1st column)
                period_value = row.find_element(By.XPATH, ".//td[4]").text.strip()  # Get the Period (4th column)

                # Try to locate the XML link in the 7th column
                try:
                    xml_link = row.find_element(By.XPATH, ".//td[7]//a")
                    driver.execute_script("arguments[0].scrollIntoView(true);", xml_link)  # Scroll to the element
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable(xml_link))  # Wait until clickable
                    driver.execute_script("arguments[0].click();", xml_link)  # Click the link using JS
                    time.sleep(2)
                except Exception as e:
                    print(f"No XML link found in row {i}. Error: {str(e)}")
                    continue  # Skip this row if the XML link is missing

                driver.switch_to.window(driver.window_handles[-1])  # Switch to the new window that opens
                current_url = driver.current_url
                print(f"Current URL: {current_url}")

                try:
                    # Extract the XML content directly
                    xml_div = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'webkit-xml-viewer-source-xml')))

                    xml_content = xml_div.get_attribute('innerHTML')  # Extract the XML content

                    # Match the symbol using the security code from the Excel sheet
                    symbol = None  # Default to None if no match is found
                    for index, excel_row in df.iterrows():
                        if str(excel_row['Security Code']) == security_code:
                            symbol = excel_row['Symbol']  # Get Symbol from the matching row in the Excel sheet
                            serial = str(excel_row['Sr. No.'])  # Extract Serial Number
                            break

                    # Proceed only if the symbol is found
                    if symbol:
                        # Save the XML file and move it to the correct folder
                        save_xml(symbol, period_value, xml_content, serial)
                    else:
                        print(f"No matching Symbol found for Security Code {security_code} in Excel sheet.")

                except Exception as e:
                    print(f"Error occurred while extracting XML content for {security_code}: {str(e)}")

                driver.close()  # Close the current window
                driver.switch_to.window(driver.window_handles[0])  # Switch back to the main window
                time.sleep(1)

            except Exception as e:
                print(f"Error occurred in row {i}: {str(e)}")

    except Exception as e:
        print(f"Error occurred during XML extraction: {str(e)}")

def main():
    """
    Main function to set up the WebDriver, navigate to the URL, and start the XML extraction process.
    """
    # URL for the webpage to scrape
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"

    # Set up Chrome options for headless browsing (if needed)
    options = Options()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")  # Uncomment this line if you want headless mode

    # Initialize the Chrome WebDriver using webdriver_manager and Service
    driver = webdriver.Chrome(options=options)

    # Navigate to the target URL
    driver.get(Top_URL)

    try:
        # Start the XML extraction process
        XML_extraction(driver)

    finally:
        # Close the WebDriver after extraction is complete
        driver.quit()

    print("Process complete")

if __name__ == "__main__":
    main()
