
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Define the folder path where you want to save the XML files
save_folder = r"D:\Consolidated_xml_file\downloads"  # Update this to your desired folder path
os.makedirs(save_folder, exist_ok=True)  # Ensure the folder exists

# Load the Excel Sheet for Security Codes and Symbols
file_path = r'D:\Consolidated_xml_file\Samples500_v2.xlsx'  # Update the path to your Excel file
df = pd.read_excel(file_path)

def save_xml(symbol, period_value, xml_content):
    """
    Save the extracted XML content to a file with the symbol and period value as the filename.
    """
    # Generate the filename with Symbol and Period
    xml_filename = os.path.join(save_folder, f"{symbol}_{period_value}.xml")

    # Save the XML content to the file
    with open(xml_filename, "w", encoding="utf-8") as file:
        file.write(xml_content)

    print(f"Con XBRL data for {symbol} saved as {xml_filename}")

def XML_extraction(driver):
    """
    Extract XML data from the website, match it with symbol from Excel sheet, and save it.
    """
    try:
        # Find all XML links on the page
        rows = driver.find_elements(By.XPATH, "//*[@id='ContentPlaceHolder1_gvData']/tbody/tr")  # Get all rows in the table
        time.sleep(1)

        for i, row in enumerate(rows):
            try:
                # Get the security code and period for matching
                security_code = row.find_element(By.XPATH, ".//td[1]").text.strip()  # Get the Security Code (1st column)
                period_value = row.find_element(By.XPATH, ".//td[4]").text.strip()  # Get the Period (4th column)

                # Check if there is a link in the 6th column (XML link)
                xml_link = row.find_element(By.XPATH, ".//td[7]//a")
                file_name = xml_link.text  # Use the link text for the file name
                print(f"Processing: {security_code}, Period: {period_value}, File: {file_name}")
                
                driver.execute_script("arguments[0].style.border='3px solid red'", xml_link)
                time.sleep(1) 

                xml_link.click()
                driver.switch_to.window(driver.window_handles[-1])
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
                            break

                    if symbol:
                        # Save the XML content using the matched symbol and period
                        save_xml(symbol, period_value, xml_content)
                    else:
                        print(f"No matching Symbol found for Security Code {security_code} in Excel sheet.")

                except Exception as e:
                    print(f"Error occurred while extracting XML content for {security_code}: {str(e)}")

                driver.close()
                driver.switch_to.window(driver.window_handles[0])
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
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

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
