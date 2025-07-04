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
from fuzzywuzzy import process

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("gemini_api_key"))

# Load payer and processor mapping
mapping_path = r"C:\Users\surya\Desktop\360\BPGscript\input\PayerProcessor.xlsx"
payer_df = pd.read_excel(mapping_path)
processors = payer_df["Processor"].dropna().unique()
payer_parents = payer_df["Payer Parent"].dropna().unique()
payers = payer_df["Payer"].dropna().unique()

# Paths
input_pdf_folder = r"C:\Users\surya\Desktop\360\BPGscript\trial_pdfs"
split_folder = os.path.join(input_pdf_folder, "split_pages")
output_folder = r"C:\Users\surya\Desktop\360\BPGscript\output"
os.makedirs(split_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

output_excel_path = os.path.join(output_folder, "payer_data_trial.xlsx")
checkpoint_path = os.path.join(output_folder, "checkpoint_processed_files.json")

# Load previous progress
# 1. Load data from the previous run if the output file exists
all_data = []
skipped_files = []
if os.path.exists(output_excel_path):
    print(f"📖 Loading existing data from '{output_excel_path}'...")
    try:
        # Load the main data
        df_existing = pd.read_excel(output_excel_path, sheet_name="Extracted Data")
        # Convert dataframe to list of dictionaries, handling potential NaN values
        all_data = df_existing.where(pd.notna(df_existing), None).to_dict('records')
        print(f"📊 Found {len(all_data)} existing records.")

        # Load the list of previously skipped files
        if "Skipped PDFs" in pd.ExcelFile(output_excel_path).sheet_names:
            df_skipped = pd.read_excel(output_excel_path, sheet_name="Skipped PDFs")
            skipped_files = df_skipped.to_dict('records')
            print(f"⚠️ Found {len(skipped_files)} previously skipped files.")

    except Exception as e:
        print(f"❌ Could not read existing Excel file, starting fresh. Error: {e}")
        all_data = []
        skipped_files = []

# 2. Load the checkpoint of processed file names
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        processed_files = json.load(f)
    print(f"🔁 Resuming from checkpoint. Already processed: {len(processed_files)} files.")
else:
    # If no checkpoint, but we have data, create a list of processed files from the data
    if all_data:
        processed_files = list(set(d['Document Name'] for d in all_data if 'Document Name' in d))
        print(f"📝 Re-created processed file list from existing data: {len(processed_files)} files.")
    else:
        processed_files = []

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

# Process PDFs
for pdf_file in os.listdir(input_pdf_folder):
    if not pdf_file.lower().endswith(".pdf"):
        continue
    if pdf_file in processed_files:
        print(f"⏭️ Skipping already processed: {pdf_file}")
        continue

    print(f"🔍 Processing: {pdf_file}")
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
        continue

    document_level_data = {
        "Payer Name": None,
        "Payer Parent Name": None,
        "Processor Name": None,
        "Effective Date": None,
        "Channel": None,
        "Sub-Channel": None,
        "Address": None,
        "Phone Number": None
    }

    for pdf_path in individual_page_paths:
        document_name = pdf_file
        match = re.search(r'_page_(\d+)\.pdf$', pdf_path)
        page_number = int(match.group(1)) if match else -1

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()

                    tables = page.extract_tables()
                    table_found = any(tables)

                    if table_found:
                        print(f"📊 Table detected on page {page_number}, switching to Camelot.")
                        try:
                            camelot_tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice", strip_text='\n')
                            if camelot_tables:
                                text_parts = []
                                for table in camelot_tables:
                                    df = table.df
                                    for _, row in df.iterrows():
                                        line = " | ".join(row.astype(str))
                                        text_parts.append(line)
                                text = "\n".join(text_parts)
                            else:
                                print("⚠️ Camelot found no tables. Falling back to normal text.")
                        except Exception as e:
                            print(f"⚠️ Camelot failed: {e}")
                    text = text or ""

                    if not text.strip():
                        continue

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

Known matches found in this page (from external reference list):
- Matched Payer Names: {matched_payers_str}
- Matched Payer Parent Names: {matched_payer_parents_str}
- Matched Processor Names: {matched_processor_str}

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
- If Plan names span multiple lines, merge them logically.
- BIN is a 6-digit numeric field.
- If multiple BIN–PCN–GRP combos are shown, extract all as separate rows.
- For Channel and SubChannel:
    - If both are present, return both.
    - If only SubChannel is found, leave Channel blank.
    - Do NOT infer missing values.

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
    "SubChannel": "..."
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
                            print("✅ Gemini content:\n", content)
                            try:
                                cleaned_content = clean_json_text(content)
                                page_data = json.loads(cleaned_content)
                            except json.JSONDecodeError as e:
                                print(f"❌ JSON decode failed: {e}")
                                continue
                        else:
                            print(f"❌ Empty or invalid response from Gemini for '{document_name}' page {page_number}")
                            continue

                        if isinstance(page_data, list):
                            for entry in page_data:
                                entry["Page Number"] = page_number
                                for key in document_level_data:
                                    if entry.get(key) and not document_level_data[key]:
                                        document_level_data[key] = entry[key].strip()
                                doc_level_keys_to_copy = set(document_level_data.keys()) - {"Channel", "SubChannel"}

                                for key in doc_level_keys_to_copy:
                                    if document_level_data[key]:
                                        entry[key] = document_level_data[key]
                                entry["Document Name"] = document_name
                                entry["Matched Payer Parents"] = matched_payer_parents_str
                                entry["Matched Payer Names"] = matched_payers_str
                                entry["Matched Processor Name"] = matched_processor_str

                            all_data.extend(page_data)

                    except Exception as e:
                        print(f"❌ Gemini API failed on '{document_name}', page {page_number}: {e}")

        except Exception as e:
            print(f"❌ Failed to open page PDF '{document_name}': {e}")
            continue

    processed_files.append(pdf_file)
    with open(checkpoint_path, "w") as f:
        json.dump(processed_files, f)

channel_fuzzy_map = {
    # Medicare
    "medicare": "Medicare",
    "medicare advantage": "Medicare",
    "medicare part d": "Medicare",
    "medicare part c": "Medicare",
    "mapd": "Medicare",
    "snf": "Medicare",
    "dsnp": "Medicare",
    "ma-pd": "Medicare",

    # Medicaid
    "medicaid": "Medicaid",
    "chip": "Medicaid",
    "medical assistance": "Medicaid",
    "state funded": "Medicaid",
    "mcd": "Medicaid",

    # Commercial
    "commercial": "Commercial",
    "group": "Commercial",
    "small group": "Commercial",
    "large group": "Commercial",
    "ppo": "Commercial",
    "hmo": "Commercial",

    # Employer
    "employer": "Employer",
    "employer sponsored": "Employer",
    "asoo": "Employer",
    "fehbp": "Employer",
    "fehb": "Employer",
    "union": "Employer",

    # Exchange
    "exchange": "Exchange",
    "marketplace": "Exchange",
    "aca": "Exchange",
    "affordable care act": "Exchange",
    "individual plan": "Exchange",
    "on exchange": "Exchange",
    "off exchange": "Exchange",
}

def normalize_channel(raw_val):
    if not raw_val:
        return ""
    val = raw_val.lower().strip()
    for keyword, canonical in channel_fuzzy_map.items():
        if keyword in val:
            return canonical
    return raw_val  # fallback

# --- Postprocessing ---
doc_level_fields = [
    "Processor Name", "Payer Name", "Payer Parent Name",
    "Effective Date", "Address", "Phone Number"
]
doc_groups = defaultdict(list)
for entry in all_data:
    doc_groups[entry["Document Name"]].append(entry)

final_data = []
for doc_name, entries in doc_groups.items():
    doc_level_values = {}
    for field in doc_level_fields:
        for entry in entries:
            if entry.get(field):
                doc_level_values[field] = entry[field].strip()
                break
    for entry in entries:
        for field in doc_level_fields:
            entry[field] = doc_level_values.get(field, "")

        # Normalize Channel field here
        if "Channel" in entry and entry["Channel"]:
            entry["Channel"] = normalize_channel(entry["Channel"])

        final_data.append(entry)

# Save to Excel
df_data = pd.DataFrame(final_data)
df_skipped = pd.DataFrame(skipped_files)

with pd.ExcelWriter(output_excel_path, engine='openpyxl', mode='w') as writer:
    df_data.to_excel(writer, index=False, sheet_name="Extracted Data")
    df_skipped.to_excel(writer, index=False, sheet_name="Skipped PDFs")

print(f"\n📊 Summary:")
print(f"✅ PDFs processed successfully: {len(processed_files)}")
print(f"❌ PDFs skipped due to errors: {len(skipped_files)}")
print(f"📁 Total PDFs attempted: {len(processed_files) + len(skipped_files)}")
print(f"📂 Output saved at: {output_excel_path}")