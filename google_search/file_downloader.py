"""
ENHANCED FILE DOWNLOADER with WGET/CURL
- Multiple PDF download strategies
- ID-based file naming
- No text extraction (removed)
"""

import logging
import time
import subprocess
import shutil
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import pandas as pd
import requests

# Check if wget/curl available
WGET_AVAILABLE = shutil.which('wget') is not None
CURL_AVAILABLE = shutil.which('curl') is not None

# Optional Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    SELENIUM_SUPPORT = True
except ImportError:
    SELENIUM_SUPPORT = False

logger = logging.getLogger(__name__)


class ImprovedFileDownloader:
    """Enhanced file downloader with ID-based naming."""

    def __init__(self, base_dir: Path = Path("downloads"), headless: bool = True):
        """Initialize downloader."""
        self.base_dir = Path(base_dir)

        # Simple folder structure - files only (no text extraction)
        self.files_dir = self.base_dir / "files"
        self.logs_dir = self.base_dir / "logs"

        for d in [self.files_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self.headless = headless
        self.browser = None
        self.pdf_load_wait = 5

        # Setup requests session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        logger.info(f"Downloader initialized")
        logger.info(f"  Files: {self.files_dir}")
        logger.info(f"  wget: {'Available' if WGET_AVAILABLE else 'Not found'}")
        logger.info(f"  curl: {'Available' if CURL_AVAILABLE else 'Not found'}")

        if SELENIUM_SUPPORT:
            self._setup_browser()

    def _setup_browser(self):
        """Setup Selenium browser."""
        if not SELENIUM_SUPPORT:
            return

        logger.info("Initializing browser...")
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless=new")

            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")

            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.managed_default_content_settings.popups": 2,
                "download.default_directory": str(self.files_dir.absolute()),
                "download.prompt_for_download": False,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

            service = Service(ChromeDriverManager().install())
            self.browser = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Browser ready")
        except Exception as e:
            logger.error(f"Browser init failed: {e}")
            self.browser = None

    def _detect_file_type(self, result: dict) -> str:
        """Detect file type from CSV metadata."""
        if 'mime_type' in result:
            mime = str(result['mime_type']).lower()
            if 'pdf' in mime:
                return 'pdf'

        if 'mime' in result:
            mime = str(result['mime']).lower()
            if 'pdf' in mime:
                return 'pdf'

        if 'file_format' in result:
            fmt = str(result['file_format']).lower()
            if 'pdf' in fmt or 'adobe' in fmt:
                return 'pdf'

        if 'fileFormat' in result:
            fmt = str(result['fileFormat']).lower()
            if 'pdf' in fmt or 'adobe' in fmt:
                return 'pdf'

        url = result.get('link', result.get('url', ''))
        if url and url.lower().endswith('.pdf'):
            return 'pdf'

        return 'html'

    def _is_valid_pdf(self, file_path: Path) -> bool:
        """Check if file is a valid PDF."""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(5)
                return header == b'%PDF-'
        except:
            return False

    def _close_popups(self):
        """Close popups and overlays."""
        if not self.browser:
            return

        close_selectors = [
            'button[aria-label*="Close"]',
            'button[aria-label*="close"]',
            'button.close',
            'button[class*="close"]',
            '.close-button',
            'button[id*="close"]',
            'button[id*="cookie"]',
        ]

        for selector in close_selectors:
            try:
                elements = self.browser.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    try:
                        if elem.is_displayed():
                            elem.click()
                            time.sleep(0.5)
                    except:
                        pass
            except:
                pass

        try:
            self.browser.execute_script("""
                document.querySelectorAll('.modal-backdrop, .overlay, [class*="popup"]').forEach(el => {
                    el.remove();
                });
                document.body.style.overflow = 'auto';
            """)
        except:
            pass

    def _download_with_wget(self, url: str, file_path: Path) -> bool:
        """Strategy: Use wget to download PDF."""
        if not WGET_AVAILABLE:
            return False

        logger.debug(f"  Strategy: wget download")

        try:
            cmd = [
                'wget',
                '--timeout=30',
                '--tries=3',
                '--no-check-certificate',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                '-O', str(file_path),
                url
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0 and file_path.exists():
                if self._is_valid_pdf(file_path):
                    size = file_path.stat().st_size
                    logger.info(f"  ✓ wget download successful: {size:,} bytes")
                    return True
                else:
                    logger.debug(f"  ✗ wget downloaded file is not a valid PDF")
                    file_path.unlink()
                    return False
            else:
                logger.debug(f"  ✗ wget failed")
                if file_path.exists():
                    file_path.unlink()
                return False

        except Exception as e:
            logger.debug(f"  ✗ wget error: {e}")
            if file_path.exists():
                file_path.unlink()
            return False

    def _download_with_curl(self, url: str, file_path: Path) -> bool:
        """Strategy: Use curl to download PDF."""
        if not CURL_AVAILABLE:
            return False

        logger.debug(f"  Strategy: curl download")

        try:
            cmd = [
                'curl',
                '--max-time', '60',
                '--retry', '3',
                '--location',
                '--insecure',
                '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                '-o', str(file_path),
                url
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)

            if result.returncode == 0 and file_path.exists():
                if self._is_valid_pdf(file_path):
                    size = file_path.stat().st_size
                    logger.info(f"  ✓ curl download successful: {size:,} bytes")
                    return True
                else:
                    logger.debug(f"  ✗ curl downloaded file is not a valid PDF")
                    file_path.unlink()
                    return False
            else:
                logger.debug(f"  ✗ curl failed")
                if file_path.exists():
                    file_path.unlink()
                return False

        except Exception as e:
            logger.debug(f"  ✗ curl error: {e}")
            if file_path.exists():
                file_path.unlink()
            return False

    def _download_pdf_direct(self, url: str, file_path: Path) -> bool:
        """Strategy: Direct download with requests."""
        logger.debug(f"  Strategy: requests direct download")

        try:
            response = self.session.get(url, timeout=30, stream=True, allow_redirects=True)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if self._is_valid_pdf(file_path):
                size = file_path.stat().st_size
                logger.info(f"  ✓ Direct download successful: {size:,} bytes")
                return True
            else:
                logger.debug(f"  ✗ Downloaded file is not a valid PDF")
                file_path.unlink()
                return False

        except Exception as e:
            logger.debug(f"  ✗ Direct download failed: {e}")
            if file_path.exists():
                file_path.unlink()
            return False

    def _find_pdf_links_in_page(self, url: str) -> List[str]:
        """Find PDF download links on page."""
        if not self.browser:
            return []

        logger.debug(f"  Strategy: Finding PDF links on page")

        try:
            self.browser.get(url)
            time.sleep(self.pdf_load_wait)
            self._close_popups()

            pdf_links = []

            selectors = [
                'a[href$=".pdf"]',
                'a[href*=".pdf"]',
                'a[href*="download"]',
                'a[href*="/pdf/"]',
                'a[aria-label*="PDF"]',
                'a[title*="PDF"]',
                'a[title*="Download"]',
                'button[aria-label*="PDF"]',
            ]

            for selector in selectors:
                try:
                    elements = self.browser.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        href = elem.get_attribute('href')
                        if href and href not in pdf_links:
                            if '.pdf' in href.lower() or 'download' in href.lower():
                                pdf_links.append(href)
                except:
                    pass

            # Check iframes
            try:
                iframes = self.browser.find_elements(By.TAG_NAME, 'iframe')
                for iframe in iframes:
                    src = iframe.get_attribute('src')
                    if src and '.pdf' in src.lower():
                        pdf_links.append(src)
            except:
                pass

            if pdf_links:
                logger.debug(f"  ✓ Found {len(pdf_links)} PDF link(s)")
            else:
                logger.debug(f"  ✗ No PDF links found")

            return list(set(pdf_links))

        except Exception as e:
            logger.debug(f"  ✗ Page search failed: {e}")
            return []

    def _save_as_html(self, url: str, filename: str) -> Optional[Path]:
        """Save page as HTML (only for non-PDF files)."""
        logger.debug(f"  Saving as HTML")

        if not self.browser:
            return None

        try:
            if self.browser.current_url != url:
                self.browser.get(url)
                time.sleep(3)

            html = self.browser.page_source

            file_path = self.files_dir / f"{filename}.html"
            counter = 1
            while file_path.exists():
                file_path = self.files_dir / f"{filename}_{counter}.html"
                counter += 1

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html)

            logger.info(f"  ✓ Saved as HTML: {file_path.name}")
            return file_path

        except Exception as e:
            logger.error(f"  ✗ HTML save failed: {e}")
            return None

    def _download_pdf_with_strategies(self, url: str, filename: str) -> Optional[Path]:
        """
        Try multiple strategies to download PDF.
        If all fail, returns None (no HTML fallback for PDFs).
        """
        logger.debug(f"Attempting PDF download with multiple strategies")

        file_path = self.files_dir / f"{filename}.pdf"
        counter = 1
        while file_path.exists():
            file_path = self.files_dir / f"{filename}_{counter}.pdf"
            counter += 1

        # Strategy 1: wget
        if WGET_AVAILABLE:
            if self._download_with_wget(url, file_path):
                return file_path

        # Strategy 2: curl
        if CURL_AVAILABLE:
            if self._download_with_curl(url, file_path):
                return file_path

        # Strategy 3: Direct download with requests
        if self._download_pdf_direct(url, file_path):
            return file_path

        # Strategy 4: Find PDF links on page
        if SELENIUM_SUPPORT:
            pdf_links = self._find_pdf_links_in_page(url)
            for link in pdf_links:
                if WGET_AVAILABLE and self._download_with_wget(link, file_path):
                    return file_path
                if CURL_AVAILABLE and self._download_with_curl(link, file_path):
                    return file_path
                if self._download_pdf_direct(link, file_path):
                    return file_path

        # All strategies failed
        logger.warning(f"  ✗ All PDF download strategies failed")
        return None

    def download_result(self, result: dict, index: int = 1, use_browser: bool = False) -> Dict:
        """Download a single search result using ID for naming."""
        url = result.get('link', result.get('url', ''))
        title = result.get('title', f'document_{index}')

        # Use ID from CSV for filename
        file_id = result.get('ID', index)

        download_status = {
            'index': index,
            'ID': file_id,
            'url': url,
            'title': title,
            'file_type': None,
            'downloaded_file': None,
            'success': False,
            'error': None
        }

        if not url or not url.startswith('http'):
            download_status['error'] = "Invalid URL"
            return download_status

        logger.info(f"\n[ID:{file_id}] {title[:60]}")
        logger.info(f"  URL: {url[:70]}...")

        try:
            file_type = self._detect_file_type(result)
            download_status['file_type'] = file_type
            logger.info(f"  Type: {file_type.upper()}")

            # Use ID for filename
            filename = f"ID{file_id}"
            downloaded_file = None

            if file_type == 'pdf':
                # Try PDF download strategies
                downloaded_file = self._download_pdf_with_strategies(url, filename)
            else:
                # For HTML files, save the page
                downloaded_file = self._save_as_html(url, filename)

            if downloaded_file:
                download_status['downloaded_file'] = str(downloaded_file)
                download_status['success'] = True
                logger.info(f"  [SUCCESS] {downloaded_file.name}")
            else:
                download_status['error'] = "Download failed - PDF not accessible"
                logger.warning(f"  [FAILED] Could not download")

        except Exception as e:
            download_status['error'] = str(e)
            logger.error(f"  [ERROR] {e}")

        return download_status

    def download_from_csv(self, csv_path: Path, limit: Optional[int] = None,
                         use_browser: bool = False) -> Tuple[int, int, List[Dict]]:
        """Download files from CSV."""
        logger.info("="*70)
        logger.info(f"BATCH DOWNLOAD: {csv_path.name}")
        logger.info("="*70)

        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} results")
        except Exception as e:
            logger.error(f"CSV read failed: {e}")
            return 0, 0, []

        if limit:
            df = df.head(limit)
            logger.info(f"Limited to {len(df)} downloads")

        successful = 0
        failed = 0
        all_results = []

        for idx, row in df.iterrows():
            result = self.download_result(row.to_dict(), idx + 1, use_browser)
            all_results.append(result)

            if result['success']:
                successful += 1
            else:
                failed += 1

            time.sleep(2)

        # Save log
        log_file = self.logs_dir / f"download_log_{csv_path.stem}.csv"
        pd.DataFrame(all_results).to_csv(log_file, index=False)
        logger.info(f"\nLog saved: {log_file}")

        return successful, failed, all_results

    def close(self):
        """Cleanup."""
        if self.browser:
            try:
                self.browser.quit()
            except:
                pass
        if self.session:
            try:
                self.session.close()
            except:
                pass
