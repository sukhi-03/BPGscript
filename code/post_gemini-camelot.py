import pandas as pd
import re
from openpyxl import load_workbook

# ----------------------
# CONFIGURATION
# ----------------------
INPUT_FILE = r'D:\Projects\new\BPGscript\output\payer_data_020725.xlsx'
SHEET_NAME_OUT = 'CleanedOutput'

# ----------------------
# STEP 1: LOAD DATA
# ----------------------
INPUT_SHEET = 'Extracted Data'
df = pd.read_excel(INPUT_FILE, sheet_name=INPUT_SHEET)

# ----------------------
# STEP 2: CLEAN & SPLIT VALUES
# ----------------------
split_cols = ['BIN', 'PCN', 'GRP']

def clean_cell(value):
    if pd.isna(value):
        return []

    value = str(value)
    
    # Remove text like "(or as appears on card)" or anything after "or"
    value = re.sub(r'\bor\b.*', '', value, flags=re.IGNORECASE)
    value = re.sub(r'\(.*?\)', '', value)

    # Replace slashes with commas
    value = value.replace('/', ',')

    # Normalize all separators to comma
    value = re.sub(r'[ ,]+', ',', value)
    
    value = value.strip(', ')
    
    # Check if the cleaned value is empty
    if not value:
        return []

    return [v.strip() for v in value.split(',') if v.strip()]

# Apply to BIN/PCN/GRP
for col in split_cols:
    df[col + '_list'] = df[col].apply(clean_cell)

# ----------------------
# STEP 3: EXPLODE
# ----------------------
df = df.explode('BIN_list').explode('PCN_list').explode('GRP_list')

# Replace original values
df['BIN'] = df['BIN_list']
df['PCN'] = df['PCN_list']
df['GRP'] = df['GRP_list']
df.drop(columns=['BIN_list', 'PCN_list', 'GRP_list'], inplace=True)

# ----------------------
# STEP 4: CONVERT SPECIAL SYMBOLS TO BLANK CELLS
# ----------------------
def convert_special_symbols_to_blank(value):
    if pd.isna(value):
        return value
    value = str(value)
    if re.match(r'^[#&\\-:]+$', value):
        return ''
    return value

# Apply to all columns
for col in df.columns:
    df[col] = df[col].apply(convert_special_symbols_to_blank)

# ----------------------
# STEP 5: WRITE TO NEW SHEET IN SAME FILE
# ----------------------
with pd.ExcelWriter(INPUT_FILE, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df.to_excel(writer, sheet_name=SHEET_NAME_OUT, index=False)

print(f"✅ Cleaned and exploded BIN/PCN/GRP columns. Output written to '{SHEET_NAME_OUT}' in the same file.")