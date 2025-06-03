import os
import re
import json
import pdfplumber
import pandas as pd
from dotenv import load_dotenv
import ollama
import time

# Load environment variables
load_dotenv()

# Constants
PDF_FOLDER = '/home/asura/Desktop/360/BPGscript/pdfs'
MAPPING_PATH = '/home/asura/Desktop/360/BPGscript/input/PayerProcessor.xlsx'
OUTPUT_FILE = os.path.join(PDF_FOLDER, "/home/asura/Desktop/360/BPGscript/output/payer_data_llm_cleaned_check2.xlsx")
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


def detect_table_structure(text):
    """Detect if the text contains a structured table like BIN/PCN combinations"""
    # Look for table headers or structured data patterns
    table_indicators = [
        r'BIN.*PCN.*COMBINATIONS',
        r'BIN\s+Processor\s+Control\s+Number',
        r'^\s*BIN\s+.*\s+Note\s*$',
        r'^\s*\d{5,6}\s+[A-Z]{2,}\s*$'  # Pattern like "610415 PCS"
    ]
    
    for pattern in table_indicators:
        if re.search(pattern, text, re.MULTILINE | re.IGNORECASE):
            return True
    return False


def extract_table_data(text, document_name, page_num):
    """Extract data from structured tables (like BIN/PCN combinations)"""
    
    prompt = f"""
You are analyzing a structured table from a payer document. This appears to be a BIN/PCN combination table.

IMPORTANT INSTRUCTIONS:
1. This is a TABLE with columns: BIN | Processor Control Number | Note
2. Each row represents ONE plan configuration
3. BIN numbers are 5-6 digits (like 610415, 004336)
4. Processor Control Numbers (PCN) are short codes (like PCS, ADV, FEPRX, etc.)
5. When you see multiple PCNs under one BIN, create separate entries for each BIN-PCN combination

Extract each BIN-PCN combination as a separate plan entry.

EXAMPLE: If you see:
BIN: 610415, PCN: PCS/ADV/RXSADV/DCADV
Create separate entries:
- BIN: 610415, PCN: PCS
- BIN: 610415, PCN: ADV  
- BIN: 610415, PCN: RXSADV
- BIN: 610415, PCN: DCADV

Return ONLY a JSON array like:
[
  {{
    "Payer Name": "CVS Caremark",
    "Plan Name/Group Name": "Legacy PCS",
    "Type Of Plan": "",
    "BIN": "610415",
    "PCN": "PCS",
    "GRP": "",
    "Effective Date": "",
    "Document Name": "{document_name}",
    "Page No.": {page_num}
  }}
]

Text:
{text}
"""

    try:
        response = ollama.chat(
            model=o_model,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response["message"]["content"]
        
        # Clean the response to extract just the JSON
        json_match = re.search(r'\[.*\]', raw, re.DOTALL)
        if json_match:
            json_str = json_match.group()
            return json.loads(json_str)
        else:
            return json.loads(raw)
            
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode failed on page {page_num} of {document_name}")
        print("Raw output:\n", raw[:500])
        return []
    except Exception as e:
        print(f"❌ Ollama request failed on page {page_num} of {document_name}: {e}")
        return []


def extract_plan_data(text, document_name, page_num):
    """Extract regular plan data (non-table format)"""
    
    prompt = f"""
You are given a page of text extracted from a payer PDF document. Extract the following fields for each **plan** found on this page.

Fields to extract (if available):
- Payer Name
- Plan Name/Group Name  
- Type Of Plan
- BIN (5-6 digit numbers only)
- PCN (short codes like PCS, ADV, FEPRX)
- GRP (group codes)
- Effective Date
- Document Name: "{document_name}"
- Page No.: {page_num}

IMPORTANT: 
- Only put actual plan names in "Plan Name/Group Name" field
- BIN should contain ONLY 5-6 digit numbers
- PCN should contain ONLY short letter codes
- Don't put BIN numbers in plan name fields

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
            model=o_model,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response["message"]["content"]
        
        # Clean the response to extract just the JSON
        json_match = re.search(r'\[.*\]', raw, re.DOTALL)
        if json_match:
            json_str = json_match.group()
            return json.loads(json_str)
        else:
            return json.loads(raw)
            
    except json.JSONDecodeError:
        print(f"❌ JSON decode failed on page {page_num} of {document_name}")
        print("Raw output:\n", raw[:500])
        return []
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
            
            # Choose extraction method based on content type
            if detect_table_structure(fixed):
                print(f"  📊 Detected table structure on page {i+1}")
                page_data = extract_table_data(fixed, document_name, i + 1)
            else:
                page_data = extract_plan_data(fixed, document_name, i + 1)

            # Add metadata to each entry
            for entry in page_data:
                entry["Document Name"] = document_name
                entry["Page No."] = i + 1
                entry["Matched Payer Name"] = matched_payer
                entry["Matched Processor Name"] = matched_processor

            all_page_data.extend(page_data)
            
            # Add small delay to avoid overwhelming the model
            time.sleep(0.5)
            
    return all_page_data


def post_process_data(df):
    """Clean and validate extracted data"""
    
    # Clean BIN values - should only be 5-6 digits
    df['BIN'] = df['BIN'].astype(str).str.extract(r'(\d{5,6})')[0]
    
    # Clean PCN values - should be short codes
    df['PCN'] = df['PCN'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()
    df.loc[df['PCN'].str.len() > 10, 'PCN'] = ''
    
    # Remove rows where both BIN and Plan Name are empty
    df = df[~((df['BIN'].isna() | (df['BIN'] == '')) & 
              (df['Plan Name/Group Name'].isna() | (df['Plan Name/Group Name'] == '')))]
    
    return df


def main():
    all_data = []
    pdf_files = [os.path.join(PDF_FOLDER, f) for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf")]
    
    for pdf_path in pdf_files:
        print(f"🔍 Processing '{os.path.basename(pdf_path)}'...")
        data = process_pdf(pdf_path)
        all_data.extend(data)

    if all_data:
        df = pd.DataFrame(all_data)
        df = post_process_data(df)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"✅ Data saved to '{OUTPUT_FILE}'")
        print(f"📊 Total records extracted: {len(df)}")
    else:
        print("⚠️ No data extracted.")


if __name__ == "__main__":
    main()