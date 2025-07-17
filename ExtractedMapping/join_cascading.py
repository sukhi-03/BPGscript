import pandas as pd
import numpy as np

# --- CONFIGURATION ---
file_path = 'payer_data_020725_test.xlsx'
output_path = 'merged_output_BPG_fallback_cascade_batchwiseX1.csv'

# Load df2
df2 = pd.read_excel(file_path, sheet_name='Key+top10k')
df2['_original_index'] = df2.index

# Create chunk list
chunk_size = 1000
df2_chunks = np.array_split(df2, max(1, len(df2) // chunk_size + 1))

# Load df1
df1 = pd.read_excel(file_path, sheet_name='Key+extracted')

# Function to format BIN values with leading zeros to ensure they are 6 digits
def format_bin(value):
    if pd.notna(value):
        value = str(value).split('.')[0]  # Remove any decimals
        return value.zfill(6)  # Pad with leading zeros to make it 6 characters
    return None

# Apply consistent formatting to BIN columns
df2['BIN_top10k'] = df2['BIN_top10k'].apply(format_bin)
df1['BIN_extracted'] = df1['BIN_extracted'].apply(format_bin)

# Sanitize df1 join columns
for col in ['BIN_extracted', 'PCN_extracted', 'GRP_extracted']:
    if col in df1.columns:
        df1[col] = df1[col].fillna('NULL').astype(str).str.strip()

# Function to generate match keys with proper BIN formatting
def generate_match_keys(row, bin_col, pcn_col, grp_col):
    bin_val = format_bin(row[bin_col]) if pd.notna(row[bin_col]) else None
    pcn_val = str(row[pcn_col]).strip() if pd.notna(row[pcn_col]) else None
    grp_val = str(row[grp_col]).strip() if pd.notna(row[grp_col]) else None

    match_keys = {
        'BPG': f"{bin_val}_{pcn_val}_{grp_val}" if bin_val and pcn_val and grp_val else None,
        'BIN_GRP': f"{bin_val}_{grp_val}" if bin_val and grp_val else None,
        'GRP': grp_val if grp_val else None,
        'BIN_PCN': f"{bin_val}_{pcn_val}" if bin_val and pcn_val else None,
        'BIN': bin_val if bin_val else None
    }
    return match_keys

def process_chunk(df2_chunk, df1, row_offset):
    df2_chunk = df2_chunk.copy()
    df2_chunk['_row_id'] = range(row_offset, row_offset + len(df2_chunk))

    # Generate match keys for df2_chunk using top10k columns
    df2_chunk['match_keys'] = df2_chunk.apply(generate_match_keys, axis=1,
                                               args=('BIN_top10k', 'PCN_top10k', 'GRP_top10k'))

    # Generate match keys for df1 using extracted columns
    df1['match_keys'] = df1.apply(generate_match_keys, axis=1,
                                  args=('BIN_extracted', 'PCN_extracted', 'GRP_extracted'))

    all_matches = []
    matched_row_ids = set()

    # Define the matching cascade levels
    levels = [
        ('BPG', 'BPG'),
        ('BIN_GRP', 'BIN+GRP'),
        ('GRP', 'GRP'),
        ('BIN_PCN', 'BIN+PCN'),
        ('BIN', 'BIN')
    ]

    for level, key in levels:
        df2_unmatched_now = df2_chunk[~df2_chunk['_row_id'].isin(matched_row_ids)]
        if df2_unmatched_now.empty:
            break

        key_mask = df2_unmatched_now['match_keys'].apply(lambda x: x.get(key) is not None)
        df2_unmatched_with_key = df2_unmatched_now[key_mask]

        if df2_unmatched_with_key.empty:
            continue

        df1_filtered = df1[df1['match_keys'].apply(lambda x: x.get(key) is not None)]

        # Debugging: Print keys and filtered DataFrames
        print(f"Level: {level}")
        print("df2_unmatched_with_key:")
        print(df2_unmatched_with_key[['_row_id', 'match_keys']])
        print("df1_filtered:")
        print(df1_filtered[['BIN_extracted', 'PCN_extracted', 'GRP_extracted', 'match_keys']])

        # Create a temporary column for the key used in merge
        df2_unmatched_with_key['key_for_merge'] = df2_unmatched_with_key['match_keys'].apply(lambda x: x.get(key))
        df1_filtered['key_for_merge'] = df1_filtered['match_keys'].apply(lambda x: x.get(key))

        # Perform merge
        merged = df2_unmatched_with_key.merge(
            df1_filtered,
            how='inner',
            left_on='key_for_merge',
            right_on='key_for_merge',
            suffixes=('', '_matched')
        )
        if not merged.empty:
            merged['Matched_Level'] = level
            all_matches.append(merged)
            matched_row_ids.update(merged['_row_id'].unique())

    # Handle unmatched rows
    final_unmatched = df2_chunk[~df2_chunk['_row_id'].isin(matched_row_ids)]
    if not final_unmatched.empty:
        final_unmatched['Matched_Level'] = 'Unmatched'
        all_matches.append(final_unmatched)

    return pd.concat(all_matches, ignore_index=True)

# Process all chunks
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
