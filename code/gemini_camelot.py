import os
import re
import json
import pdfplumber
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
import google.generativeai as genai
from dotenv import load_dotenv
from collections import defaultdict
import camelot

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("gemini_api_key"))

# Load payer and processor mapping
mapping_path = r"D:\Projects\new\BPGscript\input\PayerProcessor.xlsx"
payer_df = pd.read_excel(mapping_path)
processors = payer_df["Processor"].dropna().unique()
payer_parents = payer_df["Payer Parent"].dropna().unique()
payers = payer_df["Payer"].dropna().unique()

# Paths
input_pdf_folder = r"D:\Projects\new\BPGscript\unique_pdfs_all"
split_folder = os.path.join(input_pdf_folder, "split_pages")
output_folder = r"D:\Projects\new\BPGscript\output"
os.makedirs(split_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

output_excel_path = os.path.join(output_folder, "payer_data_020725.xlsx")
output_json_backup_path = os.path.join(output_folder, "payer_data_020725_backup.json")
checkpoint_path = os.path.join(output_folder, "checkpoint_processed_files.json")


# --- Pre-computation and Helper Functions ---

channel_fuzzy_map = {
    # Medicare
    "medicare": "Medicare", "medicare advantage": "Medicare", "medicare part d": "Medicare",
    "medicare part c": "Medicare", "mapd": "Medicare", "snf": "Medicare",
    "dsnp": "Medicare", "ma-pd": "Medicare",
    # Medicaid
    "medicaid": "Medicaid", "chip": "Medicaid", "medical assistance": "Medicaid",
    "state funded": "Medicaid", "mcd": "Medicaid",
    # Commercial
    "commercial": "Commercial", "group": "Commercial", "small group": "Commercial",
    "large group": "Commercial", "ppo": "Commercial", "hmo": "Commercial",
    # Employer
    "employer": "Employer", "employer sponsored": "Employer", "asoo": "Employer",
    "fehbp": "Employer", "fehb": "Employer", "union": "Employer",
    # Exchange
    "exchange": "Exchange", "marketplace": "Exchange", "aca": "Exchange",
    "affordable care act": "Exchange", "individual plan": "Exchange",
    "on exchange": "Exchange", "off exchange": "Exchange",
}

def normalize_channel(raw_val):
    if not raw_val:
        return ""
    val = raw_val.lower().strip()
    for keyword, canonical in channel_fuzzy_map.items():
        if keyword in val:
            return canonical
    return raw_val  # fallback

def save_progress(all_data, skipped_files, output_excel_path, output_json_path):
    """Post-processes and saves all current data to JSON and Excel."""
    # --- Post-processing ---
    doc_level_fields = [
        "Processor Name", "Payer Name", "Payer Parent Name",
        "Effective Date", "Address", "Phone Number"
    ]
    doc_groups = defaultdict(list)
    for entry in all_data:
        if "Document Name" in entry:
            doc_groups[entry["Document Name"]].append(entry)

    final_data = []
    for doc_name, entries in doc_groups.items():
        doc_level_values = {}
        # Find the first non-empty value for each doc-level field, preferring lower page numbers
        sorted_entries = sorted(entries, key=lambda x: x.get('Page Number', 999))
        for field in doc_level_fields:
            for entry in sorted_entries:
                if entry.get(field):
                    value = entry[field]
                    doc_level_values[field] = value.strip() if isinstance(value, str) else value
                    break
        
        # Apply the found doc-level values and normalize Channel for all entries in the document
        for entry in entries:
            for field in doc_level_fields:
                entry[field] = doc_level_values.get(field, "")
            # if "Channel" in entry and entry["Channel"]:
            #     entry["Channel"] = normalize_channel(entry["Channel"])
            final_data.append(entry)

    # --- Save to Files ---
    # 1. Save to JSON backup (safer and faster)
    try:
        with open(output_json_path, "w") as f:
            json.dump({'data': final_data, 'skipped': skipped_files}, f, indent=4)
        # print(f"💾 JSON backup saved to {output_json_path}")
    except Exception as e:
        print(f"❌ Could not write JSON backup. Error: {e}")

    # 2. Save to Excel
    try:
        df_data = pd.DataFrame(final_data) if final_data else pd.DataFrame()
        df_skipped = pd.DataFrame(skipped_files) if skipped_files else pd.DataFrame()

        with pd.ExcelWriter(output_excel_path, engine='openpyxl', mode='w') as writer:
            df_data.to_excel(writer, index=False, sheet_name="Extracted Data")
            df_skipped.to_excel(writer, index=False, sheet_name="Skipped PDFs")
        # print(f"📄 Excel file updated at: {output_excel_path}")
    except Exception as e:
        print(f"❌❌❌ CRITICAL: Could not write to Excel file '{output_excel_path}'. Check if the file is open. Error: {e}")
        print("Continuing script. Progress is saved in the JSON backup.")

# --- Load Previous Progress ---
all_data = []
skipped_files = []

# 1. Try loading from the JSON backup first (more reliable)
if os.path.exists(output_json_backup_path):
    print(f"📖 Loading existing data from JSON backup '{output_json_backup_path}'...")
    try:
        with open(output_json_backup_path, "r") as f:
            backup_data = json.load(f)
            all_data = backup_data.get('data', [])
            skipped_files = backup_data.get('skipped', [])
        print(f"📊 Found {len(all_data)} existing records and {len(skipped_files)} skipped files.")
    except Exception as e:
        print(f"❌ Could not read JSON backup, will try Excel. Error: {e}")
        all_data = []
        skipped_files = []

# 2. If JSON loading failed or file doesn't exist, fall back to Excel
if not all_data and os.path.exists(output_excel_path):
    print(f"📖 Loading existing data from Excel '{output_excel_path}'...")
    try:
        df_existing = pd.read_excel(output_excel_path, sheet_name="Extracted Data")
        all_data = df_existing.where(pd.notna(df_existing), None).to_dict('records')
        print(f"📊 Found {len(all_data)} existing records.")

        if "Skipped PDFs" in pd.ExcelFile(output_excel_path).sheet_names:
            df_skipped = pd.read_excel(output_excel_path, sheet_name="Skipped PDFs")
            skipped_files = df_skipped.to_dict('records') if not df_skipped.empty else []
            print(f"⚠️ Found {len(skipped_files)} previously skipped files.")
    except Exception as e:
        print(f"❌ Could not read Excel file, starting fresh. Error: {e}")
        all_data = []
        skipped_files = []

# 3. Load the checkpoint of processed file names
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        processed_files = json.load(f)
    print(f"🔁 Resuming from checkpoint. Already processed: {len(processed_files)} files.")
else:
    processed_files = list(set(d['Document Name'] for d in all_data if 'Document Name' in d))
    if processed_files:
        print(f"📝 Re-created processed file list from existing data: {len(processed_files)} files.")
    else:
        print("🚀 Starting a new run.")

def clean_json_text(raw_text):
    cleaned = raw_text.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[len("```json"):].strip()
    if cleaned.startswith("```"):
        cleaned = cleaned[len("```"):].strip()
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    return cleaned

def clean_text(text):
    return text.replace('Ø', '0')

def fix_wrapped_lines(text):
    lines = text.split("\n")
    fixed = []
    i = 0
    while i < len(lines):
        current = lines[i].strip()
        if (i + 1 < len(lines)) and not re.search(r'\d{5,}', current):
            next_line = lines[i+1].strip()
            if len(next_line) < 40 and not re.search(r'(BIN|PCN|GRP|Effective)', next_line):
                current += " " + next_line
                i += 1
        fixed.append(current)
        i += 1
    return "\n".join(fixed)

def find_matches_with_lines(lines, reference_list):
    matches = []
    for line in lines:
        for ref in reference_list:
            if re.search(rf'\b{re.escape(ref)}\b', line, re.IGNORECASE):
                matches.append(ref)
    return list(set(matches))

# --- Main Processing Loop ---
for pdf_file in os.listdir(input_pdf_folder):
    if not pdf_file.lower().endswith(".pdf"):
        continue
    if pdf_file in processed_files:
        print(f"⏭️ Skipping already processed: {pdf_file}")
        continue

    print(f"\n🔍 Processing: {pdf_file}")
    full_pdf_path = os.path.join(input_pdf_folder, pdf_file)

    try:
        reader = PdfReader(full_pdf_path)
        individual_page_paths = []

        for i, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            page_path = os.path.join(split_folder, f"{os.path.splitext(pdf_file)[0]}_page_{i+1}.pdf")
            with open(page_path, "wb") as f:
                writer.write(f)
            individual_page_paths.append(page_path)

    except Exception as e:
        print(f"❌ Skipping file '{pdf_file}' due to read error: {e}")
        skipped_files.append({"File Name": pdf_file, "Reason": str(e)})
        processed_files.append(pdf_file) # Mark as processed to avoid retrying
        with open(checkpoint_path, "w") as f:
            json.dump(processed_files, f)
        save_progress(all_data, skipped_files, output_excel_path, output_json_backup_path)
        continue

    document_level_data = {
        "Payer Name": None, "Payer Parent Name": None, "Processor Name": None,
        "Effective Date": None, "Channel": None, "Sub-Channel": None,
        "Address": None, "Phone Number": None
    }
    file_had_data = False

    for pdf_path in individual_page_paths:
        document_name = pdf_file
        match = re.search(r'_page_(\d+)\.pdf$', pdf_path)
        page_number = int(match.group(1)) if match else -1

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    table_found = any(page.extract_tables())

                    if table_found:
                        print(f"📊 Table detected on page {page_number}, switching to Camelot.")
                        try:
                            camelot_tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice", strip_text='\n')
                            if camelot_tables and camelot_tables[0].df.shape[0] > 1:
                                text_parts = [ " | ".join(row.astype(str)) for _, row in camelot_tables[0].df.iterrows() ]
                                text = "\n".join(text_parts)
                            else:
                                print("⚠️ Camelot found no usable tables. Falling back to normal text.")
                        except Exception as e:
                            print(f"⚠️ Camelot failed: {e}")
                    text = text or ""
                    if not text.strip(): continue

                    text = clean_text(text)
                    text = fix_wrapped_lines(text)
                    lines = text.split("\n")

                    processor_matches = find_matches_with_lines(lines, processors)
                    payer_parent_matches = find_matches_with_lines(lines, payer_parents)
                    payer_matches = find_matches_with_lines(lines, payers)

                    matched_processor_str = processor_matches[0] if processor_matches else "Not Found"
                    matched_payer_parents_str = ", ".join(payer_parent_matches) if payer_parent_matches else "Not Found"
                    matched_payers_str = ", ".join(payer_matches) if payer_matches else "Not Found"

                    prompt = f"""
You are given a page of text extracted from a payer PDF document. Your task is to identify and extract structured information related to pharmacy payer plans. Use only the information visible in this page and do not infer or fabricate values.

Please extract the following data points:

 Document-level fields:
- Payer Name
- Payer Parent Name
- Processor Name
- Effective Date
- Document Name: "{document_name}"
- Channel (Line of Business): Extract if the page contains text referring to the type of insurance line, such as Medicare, Medicaid, Commercial, Employer-based, or Exchange. Do not guess — only use what is explicitly written. It is okay if the term is part of a longer phrase (e.g., “Medicare Advantage” or “ACA Exchange”).
- SubChannel (Sub-Line of Business): Extract if terms such as D-SNP, HMO-POS, PPO, Part D, Dual Eligible Only, HMO, etc. are found. This represents more specific classifications of the plan under the Channel.
- Address (if found)
- Phone Number (if found)

 Plan-level fields:
- Plan Name / Group Name
- BIN
- PCN
- GRP / Group ID

Notes:
- Do NOT invent data. Only use values present in this page.
- Extract 'Payer Name','Processor','Effective Date'/'Effective as of' ONLY if explicitly labeled (not from plan names).
- If there is no 'Effective Date' given then only look for field labeled as 'Date'.
- BIN is a 6-digit numeric field.
- If multiple BIN–PCN–GRP combos are shown, extract all as separate rows.
- Do NOT infer missing values for Channel/Subchannel.

REQUIRED OUTPUT FORMAT (JSON only, no explanations):
[
  {{
    "Payer Name": "...",
    "Payer Parent Name": "...",
    "Processor Name": "...",
    "Plan Name/Group Name": "...",
    "BIN": "...", 
    "PCN": "...", 
    "GRP": "...",
    "Effective Date": "...", 
    "Document Name": "...", 
    "Channel": "...", 
    "SubChannel": "...",
    "Address": "...", 
    "Phone Number": "..."
  }}
]

Text:
{text}
"""
                    try:
                        model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
                        response = model.generate_content(prompt, generation_config={'temperature': 0})

                        if response and response.candidates:
                            content = response.candidates[0].content.parts[0].text
                            cleaned_content = clean_json_text(content)
                            page_data = json.loads(cleaned_content)
                            
                            if isinstance(page_data, list) and page_data:
                                print(f"✅ Gemini extracted {len(page_data)} record(s) from page {page_number}.")
                                file_had_data = True
                                for entry in page_data:
                                    entry["Page Number"] = page_number
                                    entry["Document Name"] = document_name
                                    entry["Matched Payer Parents"] = matched_payer_parents_str
                                    entry["Matched Payer Names"] = matched_payers_str
                                    entry["Matched Processor Name"] = matched_processor_str
                                all_data.extend(page_data)
                        else:
                            print(f"⚠️ Empty or invalid response from Gemini for page {page_number}")
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON decode failed on page {page_number}: {e}")
                    except Exception as e:
                        print(f"❌ Gemini API failed on page {page_number}: {e}")
        except Exception as e:
            print(f"❌ Failed to open page PDF '{pdf_path}': {e}")
    
    # After processing all pages of one PDF, update checkpoint and save all data
    processed_files.append(pdf_file)
    with open(checkpoint_path, "w") as f:
        json.dump(processed_files, f)
    
    if file_had_data:
        print(f"💾 Saving progress after processing '{pdf_file}'...")
        save_progress(all_data, skipped_files, output_excel_path, output_json_backup_path)
        print(f"✨ Save complete. Total records now: {len(all_data)}")
    else:
        print(f"🥱 No new data found in '{pdf_file}'. Progress file not updated.")


# --- Final Summary ---
print(f"\n\n--- SCRIPT COMPLETE ---")
print(f"📊 Summary:")
print(f"✅ PDFs processed successfully in this run: {len(os.listdir(input_pdf_folder)) - len(processed_files) + len(skipped_files)}") # This calculation is tricky, better to just state totals
print(f"✅ Total unique PDFs processed: {len(processed_files)}")
print(f"❌ Total PDFs skipped due to errors: {len(skipped_files)}")
print(f"📁 Total PDFs in folder: {len([f for f in os.listdir(input_pdf_folder) if f.lower().endswith('.pdf')])}")
print(f"🗂️ Total records extracted: {len(all_data)}")
print(f"📂 Output saved at: {output_excel_path}")
print(f"🗄️ Backup JSON at: {output_json_backup_path}")