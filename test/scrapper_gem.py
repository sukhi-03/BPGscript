import os
import re
import json
import pdfplumber
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
import google.generativeai as genai
from dotenv import load_dotenv
 
# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("gemini_api_key_4"))  # or replace with your API key as string
 
# Load payer and processor mapping
mapping_path = r"C:\Users\Surya.Pandidhar\Desktop\Projects\BPGscript\input\PayerProcessor.xlsx"
payer_df = pd.read_excel(mapping_path)
processors = payer_df["Processor"].dropna().unique()
payer_parents = payer_df["Payer Parent"].dropna().unique()
payers = payer_df["Payer"].dropna().unique()
 
# Paths
input_pdf_folder = r"C:\Users\Surya.Pandidhar\Desktop\Projects\BPGscript\pdfs_part2(200)"
split_folder = os.path.join(input_pdf_folder, "split_pages")
output_folder = r"C:\Users\Surya.Pandidhar\Desktop\Projects\BPGscript\output"
os.makedirs(split_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)
 
output_excel_path = os.path.join(output_folder, "payer_data_gemini_part2(200).xlsx")
checkpoint_path = os.path.join(output_folder, "checkpoint_processed_files.json")
 
# Load previous progress
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        processed_files = json.load(f)
    print(f"🔁 Resuming from checkpoint. Already processed: {len(processed_files)} files.")
else:
    processed_files = []
 
all_data = []
skipped_files = []
 
def clean_json_text(raw_text):
    # Remove markdown code block markers
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
        "Effective Date": None
    }
 
    for pdf_path in individual_page_paths:
        document_name = os.path.basename(pdf_path)
 
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    if not text:
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
 
📄 Document-level fields:
- Payer Name
- Payer Parent Name
- Processor Name
- Effective Date
- Document Name: "{document_name}"
 
💊 Plan-level fields:
- Plan Name / Group Name
- BIN
- PCN
- GRP / Group ID
 
⚠️ Notes:
- Do NOT invent data. Only use values present in this page.
- Extract 'Payer Name','Processor','Effective Date'/'Effective as of' ONLY if explicitly labeled (not from plan names).
- If there is no 'Effective Date' given then only look for field labeled as 'Date'.
- If Plan names span multiple lines, merge them logically.
- BIN is a 6-digit numeric field.
- If multiple BIN–PCN–GRP combos are shown, extract all as separate rows.
 
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
    "Document Name": "..."
  }}
]
 
Text:
{text}
"""
 
                    try:
                        model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
                        response = model.generate_content(
                            prompt,
                            generation_config={'temperature': 0}
                        )
 
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
                            print(f"❌ Empty or invalid response from Gemini for '{document_name}' page {i+1}")
                            continue
 
                        if isinstance(page_data, list):
                            for entry in page_data:
                                for key in document_level_data:
                                    if entry.get(key) and not document_level_data[key]:
                                        document_level_data[key] = entry[key].strip()
                                for key, val in document_level_data.items():
                                    if val:
                                        entry[key] = val
 
                                entry["Document Name"] = document_name
                                entry["Matched Payer Parents"] = matched_payer_parents_str
                                entry["Matched Payer Names"] = matched_payers_str
                                entry["Matched Processor Name"] = matched_processor_str
 
                            all_data.extend(page_data)
 
                    except Exception as e:
                        print(f"❌ Gemini API failed on '{document_name}', page {i+1}: {e}")
 
        except Exception as e:
            print(f"❌ Failed to open page PDF '{document_name}': {e}")
            continue
 
    processed_files.append(pdf_file)
 
    with open(checkpoint_path, "w") as f:
        json.dump(processed_files, f)
 
    df_data = pd.DataFrame(all_data)
    df_skipped = pd.DataFrame(skipped_files)
    with pd.ExcelWriter(output_excel_path, engine='openpyxl', mode='w') as writer:
        df_data.to_excel(writer, index=False, sheet_name="Extracted Data")
        df_skipped.to_excel(writer, index=False, sheet_name="Skipped PDFs")
 
# Final summary
print(f"\n📊 Summary:")
print(f"✅ PDFs processed successfully: {len(processed_files)}")
print(f"❌ PDFs skipped due to errors: {len(skipped_files)}")
print(f"📁 Total PDFs attempted: {len(processed_files) + len(skipped_files)}")
print(f"📂 Output saved at: {output_excel_path}")