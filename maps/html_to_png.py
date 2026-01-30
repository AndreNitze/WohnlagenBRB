"""
Export local HTML maps to PNG screenshots with Selenium + Chrome.

Typical usage (from repo root):
    python maps\\html_to_png.py --start 4 --end 12

This script looks for HTML files like:
    brb_<number>_clusters.html

and writes PNGs next to the HTML files:
    brb_<number>_clusters.png

Requirements:
    - selenium
    - webdriver-manager
    - Google Chrome (or Chromium) installed

Notes:
    - HTML files are loaded from disk via file://, so make sure the HTML
      and any referenced assets are reachable from the same location.
    - Increase --wait if tiles or layers render slowly.
"""

from pathlib import Path
import argparse
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render brb_<n>_clusters.html files to PNG screenshots."
    )
    parser.add_argument("--start", type=int, default=4, help="First cluster number")
    parser.add_argument("--end", type=int, default=12, help="Last cluster number")
    parser.add_argument(
        "--pattern",
        default="brb_{n}_clusters.html",
        help="Input HTML filename pattern, use {n} as the placeholder",
    )
    parser.add_argument(
        "--output-pattern",
        default="brb_{n}_clusters.png",
        help="Output PNG filename pattern, use {n} as the placeholder",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=2.0,
        help="Seconds to wait after loading each HTML file",
    )
    parser.add_argument(
        "--window-size",
        default="1200,900",
        help="Browser window size, e.g. 1920,1080",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run Chrome with a visible window (for debugging)",
    )
    return parser.parse_args()


def build_driver(window_size: str, headless: bool) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument(f"--window-size={window_size}")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def main() -> None:
    args = parse_args()
    driver = build_driver(window_size=args.window_size, headless=not args.no_headless)

    try:
        for number in range(args.start, args.end + 1):
            html_path = Path(args.pattern.format(n=number)).resolve()

            if not html_path.exists():
                print(f"Skipping {html_path} (not found)")
                continue

            print(f"Processing {html_path}...")
            driver.get(html_path.as_uri())
            time.sleep(args.wait)

            png_path = Path(args.output_pattern.format(n=number))
            driver.save_screenshot(str(png_path))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
