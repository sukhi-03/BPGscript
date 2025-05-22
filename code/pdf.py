import requests
import os
import fitz  # PyMuPDF
from urllib.parse import quote
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# -------- Search Google for PDFs --------
def search_pdf_links(query, max_results=3):
    links = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
            page = context.new_page()

            try:
                page.goto(f"https://www.bing.com/search?q={quote(query)}", timeout=30000)
                page.wait_for_selector("li.b_algo a", timeout=10000)
            except PlaywrightTimeoutError:
                print(f"⚠️ Timeout during Bing search for: {query}")
                return []

            anchors = page.query_selector_all("li.b_algo a")
            for a in anchors:
                href = a.get_attribute("href")
                if href and ".pdf" in href.lower() and href.startswith("http"):
                    links.append(href)
                    if len(links) >= max_results:
                        break

            browser.close()
    except Exception as e:
        print(f"❌ Error in search_pdf_links(): {e}")
        return []

    return links

# -------- Download PDF --------
def download_pdf(url):
    try:
        os.makedirs("pdfs", exist_ok=True)
        from pathlib import Path
        filename = Path("pdfs") / Path(url).name.split("?")[0]

        if os.path.exists(filename):
            return filename
        r = requests.get(url, timeout=10)
        with open(filename, "wb") as f:
            f.write(r.content)
        return filename
    except Exception as e:
        print(f"Download failed for {url}: {e}")
        return None

# -------- Extract Text from PDF --------
def extract_text_from_pdf(path):
    text = ""
    try:
        if not os.path.exists(path):
            print(f"❌ PDF file not found: {path}")
            return ""

        doc = fitz.open(path)
        for page in doc:
            text += page.get_text()
        return text[:6000]  # truncate for LLM prompt size limit
    except Exception as e:
        print(f"Failed to extract text from {path}: {e}")
        return ""