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
OUTPUT_FILE = os.path.join(PDF_FOLDER, "/home/asura/Desktop/360/BPGscript/output/payer_data_llm_cleaned_check3.xlsx")
o_model = 'llama3.1:8b'

# Read payer/processor mapping
payer_df = pd.read_excel(MAPPING_PATH)

# Extract the three separate lists
payer_list = payer_df['Payer'].dropna().unique().tolist()
payer_parent_list = payer_df['Payer Parent'].dropna().unique().tolist()
processor_list = payer_df['Processor'].dropna().unique().tolist()

# Combine all names for comprehensive matching
all_payer_names = list(set(payer_list + payer_parent_list))
all_processor_names = processor_list


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
    """Enhanced matching using the three separate lists"""
    lowered = text.lower()
    matched_payer = ""
    matched_processor = ""
    matched_parent = ""
    
    # Check for payer names (including partial matches)
    for payer in all_payer_names:
        if payer and len(str(payer)) > 3:  # Avoid very short matches
            payer_lower = str(payer).lower()
            if payer_lower in lowered:
                matched_payer = payer
                break
    
    # Check for processor names
    for processor in all_processor_names:
        if processor and len(str(processor)) > 3:
            processor_lower = str(processor).lower()
            if processor_lower in lowered:
                matched_processor = processor
                break
    
    # Check for parent company names
    for parent in payer_parent_list:
        if parent and len(str(parent)) > 3:
            parent_lower = str(parent).lower()
            if parent_lower in lowered:
                matched_parent = parent
                break
    
    return matched_payer, matched_processor, matched_parent


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
    
    # Create payer context from known payers
    payer_context = "Known payers include: " + ", ".join(all_payer_names[:10]) + "..."
    processor_context = "Known processors include: " + ", ".join(all_processor_names[:10]) + "..."
    
    prompt = f"""
You are analyzing a BIN/PCN combination table from a payer document. Look at the table structure carefully.

{payer_context}
{processor_context}

CRITICAL INSTRUCTIONS:
1. This table has columns: BIN | Processor Control Number | Note
2. Each BIN can have multiple PCNs listed vertically below it
3. When a BIN has multiple PCNs, each BIN-PCN pair is a separate entry
4. BIN numbers are 5-6 digits (610415, 004336, 610502, etc.)
5. PCNs are short codes (PCS, ADV, HNET, FEPRX24, AC, WG, FC, WK, etc.)

EXTRACTION RULES:
- If you see a BIN with multiple PCNs below it, create one entry for each BIN-PCN combination
- If you see PCNs without a direct BIN above them, look for the nearest BIN in the table structure
- Don't invent plan names - extract only what's actually shown
- Group codes (GRP) are usually separate from PCN codes
- Use known payer names from the context above when possible

EXAMPLE TABLE INTERPRETATION:
```
610415    PCS
          ADV
          RXSADV
004336    HNET
610502    FEPRX24
020099    AC
          WG  
          FC
```

This should create entries like:
- BIN: 610415, PCN: PCS
- BIN: 610415, PCN: ADV
- BIN: 610415, PCN: RXSADV
- BIN: 004336, PCN: HNET
- BIN: 610502, PCN: FEPRX24
- BIN: 020099, PCN: AC
- BIN: 020099, PCN: WG
- BIN: 020099, PCN: FC

IMPORTANT: Do not include any explanation or extra text. Return only a JSON array of dictionaries like:
[
  {{
    "Payer Name": "",
    "Plan Name/Group Name": "",
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
- PCN should contain ONLY short letter codes, sometimes with number or only numbers
- Don't put BIN numbers in plan name fields

IMPORTANT: Do not include any explanation or extra text. Return ONLY a JSON array of dictionaries like:
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
            matched_payer, matched_processor, matched_parent = match_payer_and_processor(fixed)
            
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
                entry["Matched Parent Name"] = matched_parent
                
                # If no payer name was extracted but we found a match, use it
                if not entry.get("Payer Name") and matched_payer:
                    entry["Payer Name"] = matched_payer

            all_page_data.extend(page_data)
            
            # Add small delay to avoid overwhelming the model
            time.sleep(0.5)
            
    return all_page_data


def post_process_data(df):
    """Clean and validate extracted data"""
    
    # Clean BIN values - should only be 5-6 digits
    df['BIN'] = df['BIN'].astype(str).str.extract(r'(\d{5,6})')[0]
    
    # Clean PCN values - should be short codes, remove long descriptive text
    df['PCN'] = df['PCN'].astype(str)
    # Remove entries where PCN contains long descriptive text (more than 15 chars or contains spaces)
    mask = (df['PCN'].str.len() <= 15) & (~df['PCN'].str.contains(r'\s+', na=False))
    df.loc[~mask, 'PCN'] = ''
    
    # Clean GRP column - move long text to notes or remove
    df['GRP'] = df['GRP'].astype(str)
    # If GRP contains long descriptive text, clear it
    mask = df['GRP'].str.contains(r'Group ID|Required|when', case=False, na=False)
    df.loc[mask, 'GRP'] = ''
    
    # Clean Plan Name/Group Name - remove redundant "Legacy" prefix if it's added to everything
    df['Plan Name/Group Name'] = df['Plan Name/Group Name'].astype(str)
    # If most entries start with "Legacy", it might be an artifact
    legacy_count = df['Plan Name/Group Name'].str.startswith('Legacy', na=False).sum()
    total_count = len(df[df['Plan Name/Group Name'].notna()])
    if legacy_count > total_count * 0.8:  # If more than 80% start with Legacy
        df['Plan Name/Group Name'] = df['Plan Name/Group Name'].str.replace(r'^Legacy\s*', '', regex=True)
    
    # Remove rows where both BIN and Plan Name are empty
    df = df[~((df['BIN'].isna() | (df['BIN'] == '')) & 
              (df['Plan Name/Group Name'].isna() | (df['Plan Name/Group Name'] == '')))]
    
    # For rows with PCN but no BIN, try to find the most recent BIN in the same page
    for idx, row in df.iterrows():
        if pd.isna(row['BIN']) and not pd.isna(row['PCN']) and row['PCN'] != '':
            # Look for the most recent BIN in the same page
            same_page = df[(df['Page No.'] == row['Page No.']) & (df.index < idx)]
            recent_bin = same_page[same_page['BIN'].notna() & (same_page['BIN'] != '')]
            if not recent_bin.empty:
                df.at[idx, 'BIN'] = recent_bin.iloc[-1]['BIN']
    
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