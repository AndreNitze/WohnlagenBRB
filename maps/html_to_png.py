import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from pathlib import Path

options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1200,900")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

# Define range of numbers
START = 4
END = 12

try:
    for number in range(START, END + 1):
        html_path = Path(f"brb_{number}_clusters.html").resolve()

        if not html_path.exists():
            print(f"Skipping {html_path} (not found)")
            continue

        print(f"Processing {html_path}...")
        driver.get(html_path.as_uri())
        time.sleep(2)

        png_path = f"brb_{number}_clusters.png"
        driver.save_screenshot(png_path)

finally:
    driver.quit()