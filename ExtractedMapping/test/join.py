import pandas as pd
import os

# === CONFIG ===
top10k_file = 'payer_data_020725_test.xlsx'
sheet_top10k = 'Key+top10k'
sheet_extracted = 'Key+extracted'
chunk_size = 10
output_csv = 'top10k_matches_streamed.csv'

# === CLEAN SETUP ===
if os.path.exists(output_csv):
    os.remove(output_csv)

# === LOAD extracted FULLY ===
extracted = pd.read_excel(top10k_file, sheet_name=sheet_extracted)

# Ensure string types for matching columns
for col in ['BIN_extracted', 'PCN_extracted', 'GRP_extracted']:
    extracted[col] = extracted[col].astype(str).fillna('').str.zfill(6 if 'BIN' in col else 0)

# === LOAD top10k FULLY ===
top10k = pd.read_excel(top10k_file, sheet_name=sheet_top10k)

# Ensure string types for matching columns
for col in ['BIN_top10k', 'PCN_top10k', 'GRP_top10k']:
    top10k[col] = top10k[col].astype(str).fillna('').str.zfill(6 if 'BIN' in col else 0)

# === DEFINE MATCH LEVELS ===
def get_match_levels(BIN, PCN, GRP):
    levels = []
    if BIN and PCN and GRP:
        levels.append(('BIN+PCN+GRP', (BIN, PCN, GRP)))
    if BIN and GRP:
        levels.append(('BIN+GRP', (BIN, GRP)))
    if GRP:
        levels.append(('GRP', (GRP,)))
    if BIN and PCN:
        levels.append(('BIN+PCN', (BIN, PCN)))
    if BIN:
        levels.append(('BIN', (BIN,)))
    return levels

# === PROCESS IN CHUNKS ===
row_offset = 0
for start in range(0, len(top10k), chunk_size):
    chunk = top10k.iloc[start:start + chunk_size].copy()
    chunk_matches = []

    for i, row in chunk.iterrows():
        index = row_offset + i
        BIN = row['BIN_top10k']
        PCN = row['PCN_top10k']
        GRP = row['GRP_top10k']
        matched = False

        for level, keys in get_match_levels(BIN, PCN, GRP):
            if level == 'BIN+PCN+GRP':
                match_df = extracted[
                    (extracted['BIN_extracted'] == keys[0]) &
                    (extracted['PCN_extracted'] == keys[1]) &
                    (extracted['GRP_extracted'] == keys[2])
                ]
            elif level == 'BIN+GRP':
                match_df = extracted[
                    (extracted['BIN_extracted'] == keys[0]) &
                    (extracted['GRP_extracted'] == keys[1])
                ]
            elif level == 'GRP':
                match_df = extracted[
                    (extracted['GRP_extracted'] == keys[0])
                ]
            elif level == 'BIN+PCN':
                match_df = extracted[
                    (extracted['BIN_extracted'] == keys[0]) &
                    (extracted['PCN_extracted'] == keys[1])
                ]
            elif level == 'BIN':
                match_df = extracted[
                    (extracted['BIN_extracted'] == keys[0])
                ]
            else:
                match_df = pd.DataFrame()

            if not match_df.empty:
                match_df = match_df.copy()
                match_df['MatchLevel'] = level
                match_df['Top10kIndex'] = index
                match_df['BIN_top10k'] = BIN
                match_df['PCN_top10k'] = PCN
                match_df['GRP_top10k'] = GRP
                chunk_matches.append(match_df)
                matched = True
                break  # found match, move to next row

        if not matched:
            chunk_matches.append(pd.DataFrame([{
                'MatchLevel': 'No match found',
                'Top10kIndex': index,
                'BIN_top10k': BIN,
                'PCN_top10k': PCN,
                'GRP_top10k': GRP
            }]))

    # Write current chunk matches to CSV
    if chunk_matches:
        result_df = pd.concat(chunk_matches, ignore_index=True)
        priority_cols = ['Top10kIndex', 'BIN_top10k', 'PCN_top10k', 'GRP_top10k', 'MatchLevel']
        result_df = result_df[priority_cols + [col for col in result_df.columns if col not in priority_cols]]

        if not os.path.exists(output_csv):
            result_df.to_csv(output_csv, index=False)
        else:
            result_df.to_csv(output_csv, mode='a', index=False, header=False)

    row_offset += len(chunk)

print(f"✅ Done! Output saved to: {output_csv}")
