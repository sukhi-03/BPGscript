import pandas as pd

merged_file = "merged_output_BPG_fallback_cascade_batchwise3.csv"
df_merged = pd.read_csv(merged_file, low_memory=False)

rule_col_candidates = ['Matched_Level', 'rule_applied', 'rule_used']
rule_col = next((col for col in rule_col_candidates if col in df_merged.columns), None)
if not rule_col:
    raise ValueError("❌ No rule match column found. Please ensure your join script includes rule name per match.")

df_merged[rule_col] = df_merged[rule_col].fillna('No Match')

df_matched = df_merged[df_merged[rule_col] != 'Unmatched']
df_matched = df_matched[df_matched[rule_col] != 'No Match']

summary = (
    df_matched
    .groupby(['BPG_top10k'])[rule_col]
    .agg([
        ('Match_Count', 'count'),
        ('Rules_Used', lambda x: ', '.join(sorted(set(x))))
    ])
    .reset_index()
)

df2 = pd.read_excel('payer_data_020725_test.xlsx', sheet_name='Key+top10k')
df2['_original_index'] = df2.index

summary = df2[['BPG_top10k','_original_index']].drop_duplicates().merge(summary, on='BPG_top10k', how='left')
summary = summary.sort_values('_original_index').drop(columns=['_original_index'])

summary['Match_Count'] = summary['Match_Count'].fillna(0).astype(int)
summary['Rules_Used'] = summary['Rules_Used'].fillna('No Match')

summary.to_csv("summary_match_counts_BPG_final3.csv", index=False)
print("✅ Summary saved to: summary_match_counts_BPG_final3.csv")