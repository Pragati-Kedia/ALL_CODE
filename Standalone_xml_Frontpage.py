import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

# Define the folder path where you want to save the XML files (already existing folder)
save_folder = r"D:\FinancialStatementAnalysis\01ETL\extracted"  # Base folder where XML files will be saved
log_path = r"D:\FinancialStatementAnalysis\04log"
# Load the Excel Sheet for Security Codes and Symbols
file_path = r"D:\FinancialStatementAnalysis\03input\Samples500_v2.xlsx"  # Path to your Excel file
df = pd.read_excel(file_path)

# Define log file name and path
today_date = datetime.now().strftime("%Y-%m-%d")
log_file_path = os.path.join(log_path, f"frontpage_{today_date}.xlsx")
log_data = []  # To store log information

def save_xml(symbol, period_value, xml_content, sr_no):
    """
    Save the extracted XML content to a file and move it to the corresponding symbol folder.
    """
    # Construct folder name based on Symbol and Sr No
    folder_name = f"{sr_no}_{symbol}"
    symbol_folder_path = os.path.join(save_folder, folder_name)
    os.makedirs(symbol_folder_path, exist_ok=True)  # Ensure the folder exists before saving

    # Generate the filename for the XML file using Symbol and Period
    xml_filename = os.path.join(symbol_folder_path, f"{symbol}_{period_value}.xml")

    # Save the XML content to the file
    with open(xml_filename, "w", encoding="utf-8") as file:
        file.write(xml_content)

    print(f"XML data for {symbol} saved as {xml_filename}")

def save_log_file():
    """
    Save the log data to an Excel file.
    """
    log_df = pd.DataFrame(log_data, columns=["Sr. No.", "Symbol", "Period", "Status"])
    log_df.to_excel(log_file_path, index=False)
    print(f"Log file saved at {log_file_path}")

def XML_extraction(driver):
    """
    Extract XML data from the website, match it with the symbol from the Excel sheet, and save it.
    """
    try:
        # Find all rows in the table
        rows = driver.find_elements(By.XPATH, "//*[@id='ContentPlaceHolder1_gvData']/tbody/tr")
        time.sleep(1)

        for i, row in enumerate(rows):
            try:
                # Get the security code and period for matching
                security_code = row.find_element(By.XPATH, ".//td[1]").text.strip()  # Get the Security Code (1st column)
                period_value = row.find_element(By.XPATH, ".//td[4]").text.strip()  # Get the Period (4th column)

                # Try to locate the XML link in the 7th column
                try:
                    xml_link = row.find_element(By.XPATH, ".//td[6]//a")
                    driver.execute_script("arguments[0].scrollIntoView(true);", xml_link)  # Scroll to the element
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable(xml_link))  # Wait until clickable
                    driver.execute_script("arguments[0].click();", xml_link)  # Click the link using JS
                    time.sleep(2)
                except Exception as e:
                    print(f"No XML link found in row {i}. Error: {str(e)}")
                    log_data.append(["N/A", "N/A", period_value, "No XML link found"])
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
                    sr_no = None
                    for index, excel_row in df.iterrows():
                        if str(excel_row['Security Code']) == security_code:
                            symbol = excel_row['Symbol']  # Get Symbol from the matching row in the Excel sheet
                            sr_no = str(excel_row['Sr. No.'])  # Extract Sr No
                            break

                    # Proceed only if the symbol is found
                    if symbol:
                        # Save the XML file and move it to the correct folder
                        save_xml(symbol, period_value, xml_content, sr_no)
                        log_data.append([sr_no, symbol, period_value, "Success"])
                    else:
                        print(f"No matching Symbol found for Security Code {security_code} in Excel sheet.")
                        log_data.append(["N/A", "N/A", period_value, "No matching Symbol found"])

                except Exception as e:
                    print(f"Error occurred while extracting XML content for {security_code}: {str(e)}")
                    log_data.append(["N/A", "N/A", period_value, "XML content extraction error"])

                driver.close()  # Close the current window
                driver.switch_to.window(driver.window_handles[0])  # Switch back to the main window
                time.sleep(1)

            except Exception as e:
                print(f"Error occurred in row {i}: {str(e)}")
                log_data.append(["N/A", "N/A", "N/A", f"Row error: {str(e)}"])

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
        # Save the log file
        save_log_file()

        # Close the WebDriver after extraction is complete
        driver.quit()

    print("Process complete")

if __name__ == "__main__":
    main()