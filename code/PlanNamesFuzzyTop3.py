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
OUTPUT_FILE = "matched_plans_top3.xlsx"
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
def get_top_matches_from_split(text, choices, top_n=3):
    parts = [preprocess(part) for part in str(text).split("/")]
    all_matches = []

    for part in parts:
        matches = process.extract(part, choices, scorer=fuzz.token_sort_ratio, limit=top_n)
        all_matches.extend(matches)

    # Sort all collected matches and keep top N unique matches
    unique_matches = {}
    for match_text, score, _ in sorted(all_matches, key=lambda x: x[1], reverse=True):
        if match_text not in unique_matches:
            match_row = sheet_b[sheet_b["cleaned"] == match_text]
            original_name = match_row[COL_NAME_B].values[0] if not match_row.empty else match_text
            unique_matches[match_text] = (original_name, score)
        if len(unique_matches) >= top_n:
            break

    return list(unique_matches.values())


# --------------------
# MATCHING LOOP
# --------------------
print("Matching plans with score threshold...")
matches = []

for original_text in sheet_a[COL_NAME_A]:
    top_matches = get_top_matches_from_split(original_text, plans_b_cleaned, top_n=3)

    if top_matches:
        row = [original_text]
        for i in range(3):
            if i < len(top_matches):
                row.extend([top_matches[i][0], top_matches[i][1]])  # plan name, score
            else:
                row.extend([None, None])
        matches.append(row)
    else:
        matches.append([original_text, None, None, None, None, None, None])


# --------------------
# SAVE TO EXCEL
# --------------------
print("Saving results...")
results_df = pd.DataFrame(matches, columns=[
    "SheetA_Original",
    "Match1_Plan", "Match1_Score",
    "Match2_Plan", "Match2_Score",
    "Match3_Plan", "Match3_Score"
])

results_df.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Matching complete. Results saved to: {OUTPUT_FILE}")
