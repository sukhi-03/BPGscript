import pandas as pd
import numpy as np

# --- CONFIGURATION ---
file_path = 'payer_data_020725_test.xlsx'
output_path = 'merged_output_BPG_fallback_cascade_batchwise3.csv'

# Load df2
df2 = pd.read_excel(file_path, sheet_name='Key+top10k')
df2['_original_index'] = df2.index

# Create chunk list
chunk_size = 1000
df2_chunks = np.array_split(df2, max(1, len(df2) // chunk_size + 1))

# Load df1
df1 = pd.read_excel(file_path, sheet_name='Key+extracted')

# Sanitize df1 join columns
for col in ['BPG_extracted', 'B&G_extracted', 'GRP_extracted', 'B&P_extracted', 'BIN_extracted']:
    df1[col] = df1[col].fillna('NULL').astype(str).str.strip()

# --- FUNCTION: Process a Chunk of df2 ---
def process_chunk(df2_chunk, df1, row_offset):
    df2_chunk = df2_chunk.copy()

    # Sanitize df2 join columns
    for col in ['BPG_top10k', 'B&G_top10k', 'GRP_top10k', 'B&P_top10k', 'BIN_top10k']:
        df2_chunk[col] = df2_chunk[col].fillna('NULL').astype(str).str.strip()

    # Assign global row IDs
    df2_chunk['_row_id'] = range(row_offset, row_offset + len(df2_chunk))

    # Identify totally null rows
    null_mask = df2_chunk['BPG_top10k'] == '(NULL)(NULL)(NULL)'
    df_unmatched = df2_chunk[null_mask].copy()
    df_unmatched['Matched_Level'] = 'Unmatched'

    # Proceed with matchable rows
    df2_matchable = df2_chunk[~null_mask].copy()
    all_matches = []
    matched_row_ids = set()

    def merge_and_tag(left, right, left_on, right_on, level):
        merged = left.merge(
            right,
            how='inner',
            left_on=left_on,
            right_on=right_on,
            suffixes=('', '_matched')
        )
        merged['Matched_Level'] = level
        return merged

    # Cascading match levels
    for level, left_col, right_col in [
        ('BPG', 'BPG_top10k', 'BPG_extracted'),
        ('BIN+GRP', 'B&G_top10k', 'B&G_extracted'),
        ('GRP', 'GRP_top10k', 'GRP_extracted'),
        ('BIN+PCN', 'B&P_top10k', 'B&P_extracted'),
        ('BIN', 'BIN_top10k', 'BIN_extracted'),
    ]:
        df2_unmatched_now = df2_matchable[~df2_matchable['_row_id'].isin(matched_row_ids)]
        if df2_unmatched_now.empty:
            break

        matched = merge_and_tag(df2_unmatched_now, df1, left_col, right_col, level)

        # ❗ Keep all matches per row — do NOT drop duplicates
        all_matches.append(matched)
        matched_row_ids.update(matched['_row_id'].unique())

    # Remaining unmatched rows
    final_unmatched = df2_matchable[~df2_matchable['_row_id'].isin(matched_row_ids)]
    final_unmatched['Matched_Level'] = 'Unmatched'

    # Combine results
    all_matches.extend([final_unmatched, df_unmatched])
    return pd.concat(all_matches, ignore_index=True)

# --- PROCESS ALL CHUNKS ---
first_chunk = True
global_row_offset = 0

for chunk_num, df2_chunk in enumerate(df2_chunks, start=1):
    print(f"🔄 Processing chunk {chunk_num}...")
    chunk_result = process_chunk(df2_chunk, df1, global_row_offset)
    global_row_offset += len(df2_chunk)

    chunk_result.to_csv(output_path, index=False, mode='w' if first_chunk else 'a', header=first_chunk)
    first_chunk = False

    print(f"✅ Chunk {chunk_num} saved with {len(chunk_result)} rows.")

print(f"\n✅ All chunks processed. Final output saved to:\n{output_path}")
