import os

# Set the folder containing your PDFs
folder_path = r'D:\Projects\BPGscript\vol2pdfs'

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.startswith('pdf_') and filename.endswith('.pdf'):
        old_path = os.path.join(folder_path, filename)
        new_filename = 'vol2_' + filename
        new_path = os.path.join(folder_path, new_filename)
        os.rename(old_path, new_path)
        print(f'Renamed: {filename} -> {new_filename}')
