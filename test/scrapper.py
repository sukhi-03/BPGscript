import os
import re
import json
import pdfplumber
import pandas as pd
# import google.generativeai as genai
from dotenv import load_dotenv
import ollama
import time

# Load environment variables
load_dotenv()
# api_key = os.getenv("gemini_api_key")

# Configure Gemini
# genai.configure(api_key=api_key)
# gemini_model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
# chat_session = gemini_model.start_chat()

# Constants
PDF_FOLDER = '/home/asura/Desktop/360/BPGscript/pdfs'
MAPPING_PATH = '/home/asura/Desktop/360/BPGscript/input/PayerProcessor.xlsx'
OUTPUT_FILE = os.path.join(PDF_FOLDER, "/home/asura/Desktop/360/BPGscript/output/payer_data_llm_cleaned_check.xlsx")
o_model = 'llama3.1:8b'

# Read payer/processor mapping
payer_df = pd.read_excel(MAPPING_PATH)
payer_processor_list = payer_df.to_dict(orient="records")


def clean_text(text):
    return text.replace('Ø', '0')


def fix_wrapped_lines(text):
    lines = text.split("\n")
    fixed_lines = []
    i = 0
    while i < len(lines):
        current = lines[i].strip()
        if i + 1 < len(lines) and not re.search(r'\d{5,}', current):
            next_line = lines[i + 1].strip()
            if len(next_line) < 40 and not re.search(r'(BIN|PCN|GRP|Effective)', next_line):
                current += " " + next_line
                i += 1
        fixed_lines.append(current)
        i += 1
    return "\n".join(fixed_lines)


def match_payer_and_processor(text):
    lowered = text.lower()
    matched_payer = ""
    matched_processor = ""
    for item in payer_processor_list:
        payer_name = str(item.get("Payer Name", "")).lower()
        processor_name = str(item.get("Processor Name", "")).lower()
        if payer_name and payer_name in lowered:
            matched_payer = item.get("Payer Name")
        if processor_name and processor_name in lowered:
            matched_processor = item.get("Processor Name")
    return matched_payer, matched_processor


def extract_plan_data(text, document_name, page_num):
    prompt = f"""
You are given a page of text extracted from a payer PDF document. Extract the following fields for each **plan** found on this page. Use only the information in this page.

Fields to extract (if available):
- Payer Name
- Plan Name/Group Name  
- Type Of Plan
- BIN
- PCN
- GRP
- Effective Date
- Document Name: "{document_name}"
- Page No.: {page_num}

Note: Plan names may be wrapped across multiple lines. Merge them if they appear to be part of the same plan (e.g., 'Clark County Only').

Return ONLY a JSON array of dictionaries like:
[
  {{
    "Payer Name": "...",
    "Plan Name/Group Name": "...",
    "Type Of Plan": "...",
    "BIN": "...",
    "PCN": "...",
    "GRP": "...",
    "Effective Date": "...",
    "Document Name": "...",
    "Page No.": ...
  }}
]

IMPORTANT: Return only a JSON array of dictionaries. Do not include any explanation or extra text.

Text:
{text}
"""

    try:
        response = ollama.chat(
            model= o_model,  # or your preferred model
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response["message"]["content"]
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"❌ JSON decode failed on page {page_num} of {document_name}")
        print("Raw output:\n", raw)
    except Exception as e:
        print(f"❌ Ollama request failed on page {page_num} of {document_name}: {e}")
    return []


def process_pdf(pdf_path):
    all_page_data = []
    document_name = os.path.basename(pdf_path)

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
            cleaned = clean_text(text)
            fixed = fix_wrapped_lines(cleaned)
            matched_payer, matched_processor = match_payer_and_processor(fixed)
            page_data = extract_plan_data(fixed, document_name, i + 1)

            for entry in page_data:
                entry["Document Name"] = document_name
                entry["Page No."] = i + 1
                entry["Matched Payer Name"] = matched_payer
                entry["Matched Processor Name"] = matched_processor

            all_page_data.extend(page_data)
    return all_page_data

def main():
    all_data = []
    pdf_files = [os.path.join(PDF_FOLDER, f) for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf")]
    
    for pdf_path in pdf_files:
        print(f"🔍 Processing '{os.path.basename(pdf_path)}'...")
        data = process_pdf(pdf_path)
        all_data.extend(data)

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"✅ Data saved to '{OUTPUT_FILE}'")
    else:
        print("⚠️ No data extracted.")


if __name__ == "__main__":
    main()
