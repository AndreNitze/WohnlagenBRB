from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


DEFAULT_MAPS_DIR = Path(__file__).resolve().parent / "maps"
DEFAULT_BROWSER_CANDIDATES = [
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    Path("/usr/bin/google-chrome"),
    Path("/usr/bin/google-chrome-stable"),
    Path("/usr/bin/chromium"),
    Path("/usr/bin/chromium-browser"),
    Path("/usr/bin/microsoft-edge"),
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
]
DEFAULT_BROWSER_COMMANDS = [
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "microsoft-edge",
    "msedge",
    "chrome",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render all HTML files in the maps/ directory to PNG screenshots."
    )
    parser.add_argument(
        "--maps-dir",
        type=Path,
        default=DEFAULT_MAPS_DIR,
        help="Directory containing HTML map files.",
    )
    parser.add_argument(
        "--browser",
        type=Path,
        default=None,
        help="Optional browser executable path. Defaults to Chrome/Edge autodetection.",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=1800,
        help="Browser viewport width in pixels.",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=1200,
        help="Browser viewport height in pixels.",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="Device scale factor for higher-resolution screenshots.",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=3.0,
        help="Additional wait time after page load before screenshot.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing PNG files.",
    )
    return parser.parse_args()


def find_browser(explicit_browser: Path | None) -> Path:
    if explicit_browser is not None:
        if not explicit_browser.exists():
            raise FileNotFoundError(f"Browser not found: {explicit_browser}")
        return explicit_browser

    for candidate in DEFAULT_BROWSER_CANDIDATES:
        if candidate.exists():
            return candidate

    for command in DEFAULT_BROWSER_COMMANDS:
        resolved = shutil.which(command)
        if resolved:
            return Path(resolved)

    raise FileNotFoundError(
        "No supported browser found. Pass --browser with a Chrome/Edge/Chromium executable."
    )


def build_driver(browser_path: Path, width: int, height: int, scale: int) -> webdriver.Chrome:
    options = Options()
    options.binary_location = str(browser_path)
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--force-device-scale-factor=" + str(scale))
    options.add_argument(f"--window-size={width},{height}")
    options.add_argument("--allow-file-access-from-files")
    options.add_argument("--enable-local-file-accesses")
    service = Service()
    return webdriver.Chrome(service=service, options=options)


def file_url(path: Path) -> str:
    return "file:///" + quote(str(path.resolve()).replace("\\", "/"))


def render_html_to_png(
    driver: webdriver.Chrome,
    html_path: Path,
    png_path: Path,
    wait_seconds: float,
) -> None:
    driver.get(file_url(html_path))
    time.sleep(wait_seconds)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    driver.save_screenshot(str(png_path))


def main() -> int:
    args = parse_args()
    maps_dir = args.maps_dir.resolve()
    if not maps_dir.exists():
        print(f"Maps directory not found: {maps_dir}", file=sys.stderr)
        return 1

    html_files = sorted(maps_dir.glob("*.html"))
    if not html_files:
        print(f"No HTML files found in {maps_dir}")
        return 0

    browser_path = find_browser(args.browser)
    print(f"Using browser: {browser_path}")

    driver = build_driver(browser_path, args.width, args.height, args.scale)
    try:
        for html_path in html_files:
            png_path = html_path.with_suffix(".png")
            if png_path.exists() and not args.overwrite:
                print(f"Skipping existing PNG: {png_path.name}")
                continue

            print(f"Rendering {html_path.name} -> {png_path.name}")
            render_html_to_png(driver, html_path, png_path, args.wait_seconds)
    finally:
        driver.quit()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
