import pandas as pd
import re
from rapidfuzz import fuzz, process, utils, distance
import numpy as np


# --------------------
# CONFIGURATION
# --------------------
FILE_PATH = r"D:\Projects\new\BPGscript\input\PlanNamesFuzzy.xlsx"  # Change this to your actual path
SHEET_A = "ExtractedData"
SHEET_B = "DataModel"
SHEET_ABBR = "Abb.s"
COL_NAME_A = "Plan Name/Group Name"
COL_NAME_B = "Plan"
OUTPUT_FILE = "matched_plans.xlsx"
SCORE_THRESHOLD = 85

# --------------------
# LOAD DATA
# --------------------
print("Reading Excel file...")
sheet_a = pd.read_excel(FILE_PATH, sheet_name=SHEET_A)
sheet_b = pd.read_excel(FILE_PATH, sheet_name=SHEET_B)
abbrev_df = pd.read_excel(FILE_PATH, sheet_name=SHEET_ABBR)

# --------------------
# BUILD ABBREVIATION MAP
# --------------------
abbrev_map = dict(zip(
    abbrev_df["Abbreviations"].astype(str).str.lower().str.strip(),
    abbrev_df["Full Form"].astype(str).str.lower().str.strip()
))

# --------------------
# CLEANING + NORMALIZATION
# --------------------
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r'\(.*?\)', '', text)                  # remove anything in brackets
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)           # remove special characters
    text = re.sub(r'\s+', ' ', text)                     # normalize spaces
    return text.strip()

def expand_abbreviations(text, mapping):
    words = text.split()
    expanded_words = [mapping.get(word, word) for word in words]
    return ' '.join(expanded_words)

def preprocess(text):
    return expand_abbreviations(clean_text(text), abbrev_map)

# --------------------
# PREPROCESS PLAN NAMES
# --------------------
print("Preprocessing plan names...")
sheet_a["cleaned"] = sheet_a[COL_NAME_A].astype(str).apply(preprocess)
sheet_b["cleaned"] = sheet_b[COL_NAME_B].astype(str).apply(preprocess)
plans_b_cleaned = sheet_b["cleaned"].dropna().unique().tolist()

# --------------------
# MATCH FUNCTION (Handles slash-separated names)
# --------------------
def get_best_match_from_split(text, choices):
    parts = [preprocess(part) for part in str(text).split("/")]
    best_match = None
    best_score = -1
    best_original = None
    
    for part in parts:
        match = process.extractOne(part, choices, scorer=fuzz.token_sort_ratio)
        if match:
            matched_cleaned, score, _ = match
            if score > best_score:
                best_score = score
                best_match = matched_cleaned
                match_row = sheet_b[sheet_b["cleaned"] == matched_cleaned]
                best_original = match_row[COL_NAME_B].values[0] if not match_row.empty else matched_cleaned

    return best_original, best_score

# --------------------
# MATCHING LOOP
# --------------------
print("Matching plans with score threshold...")
matches = []
for original_text in sheet_a[COL_NAME_A]:
    best_match, score = get_best_match_from_split(original_text, plans_b_cleaned)
    
    if score >= SCORE_THRESHOLD:
        matches.append((original_text, best_match, score))
    else:
        matches.append((original_text, None, score))

# --------------------
# SAVE TO EXCEL
# --------------------
print("Saving results...")
results_df = pd.DataFrame(matches, columns=["SheetA_Original", "Best_Match_SheetB", "Match_Score"])
results_df.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Matching complete. Results saved to: {OUTPUT_FILE}")
