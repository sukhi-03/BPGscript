import pandas as pd
import os

# Full path to your Excel file
excel_path = r'D:\Projects\BPGscript\vol2pdfs\Next500_link_reference.xlsx'

# Load Excel file
df = pd.read_excel(excel_path)

# Rename filenames in the first column
df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: f'vol2_{x}' if isinstance(x, str) and x.startswith('pdf_') else x)

# Save to same folder with 'updated_' prefix in filename
folder = os.path.dirname(excel_path)
filename = os.path.basename(excel_path)
new_excel_path = os.path.join(folder, 'updated_' + filename)

df.to_excel(new_excel_path, index=False)
