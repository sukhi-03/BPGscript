import os
import hashlib
import json
import shutil
import pandas as pd

# === CONFIGURATION ===
# Use a more robust way to define paths
BASE_FOLDER = r"D:\Projects\BPGscript\vol2pdfs"
input_pdf_folder = BASE_FOLDER # The main folder to scan
hash_checkpoint_path = os.path.join(BASE_FOLDER, "checkpoint_hashes.json")
duplicate_folder = os.path.join(BASE_FOLDER, "duplicates")
# Renamed output for clarity that it's a comprehensive report
comprehensive_duplicate_map_output = os.path.join(BASE_FOLDER, "duplicate_map_vol2.xlsx")

# Ensure the duplicates folder exists
os.makedirs(duplicate_folder, exist_ok=True)

# === Load existing hash record ===
# This will now be our single source of truth for all hashes seen so far
if os.path.exists(hash_checkpoint_path):
    print(f"Loading existing hash checkpoint from: {hash_checkpoint_path}")
    with open(hash_checkpoint_path, "r") as f:
        all_seen_hashes = json.load(f)
else:
    print("No checkpoint file found. Starting with a new hash record.")
    all_seen_hashes = {}

# === Function to generate MD5 file hash ===
def get_file_hash(filepath, algo="md5", block_size=65536):
    """Calculates the hash of a file."""
    hasher = hashlib.new(algo)
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(block_size):
                hasher.update(chunk)
        return hasher.hexdigest()
    except IOError as e:
        print(f"Error reading file {filepath}: {e}")
        return None # Return None if file cannot be read

# === Scan and process PDFs ===
duplicates_found_this_run = []
new_files_count = 0
# This will store data as: {'original_file.pdf': ['copy1.pdf', 'copy2.pdf']}
# This dictionary specifically tracks duplicates *found and moved in the current run*.
grouped_duplicates_this_run = {}


print("\nScanning for duplicate PDFs...")
# Iterate through all items in the folder
for filename in os.listdir(input_pdf_folder):
    filepath = os.path.join(input_pdf_folder, filename)

    # Skip directories (like 'duplicates') and non-PDF files
    if not os.path.isfile(filepath) or not filename.lower().endswith(".pdf"):
        continue

    # Calculate hash
    file_hash = get_file_hash(filepath)
    if file_hash is None: # Skip if hash calculation failed
        continue

    if file_hash in all_seen_hashes:
        # This is a duplicate (either from a past run or this run)
        original_filename_from_checkpoint = all_seen_hashes[file_hash]

        # Avoid flagging a file as a duplicate of itself if it's the original
        if filename == original_filename_from_checkpoint:
            continue

        print(f"⚠️  Duplicate detected: '{filename}' is a copy of '{original_filename_from_checkpoint}'")
        duplicates_found_this_run.append(filename)
        
        # Populate our new grouped dictionary for duplicates found *this run*
        # Use original_filename_from_checkpoint as the key, as it's the first known instance
        grouped_duplicates_this_run.setdefault(original_filename_from_checkpoint, []).append(filename)
        
        # Move the duplicate file
        try:
            shutil.move(filepath, os.path.join(duplicate_folder, filename))
        except Exception as e:
            print(f"    ERROR moving file {filename}: {e}")

    else:
        # This is a new, unique file. Add it to our dictionary for future checks.
        print(f"✅ New unique file found: '{filename}'")
        all_seen_hashes[file_hash] = filename
        new_files_count += 1

# === Update the checkpoint with all hashes (new and old) ===
print("\nUpdating hash checkpoint...")
try:
    with open(hash_checkpoint_path, "w") as f:
        json.dump(all_seen_hashes, f, indent=4)
except IOError as e:
    print(f"Error saving checkpoint file: {e}")

# === Reworked logic to save the improved comprehensive map to Excel ===
# This will now include all unique files known from the checkpoint
comprehensive_report_data = []

# Get all unique original filenames from the checkpoint
# Using set() to ensure we only process each original filename once,
# even if multiple hashes somehow pointed to it (shouldn't happen with correct hashing)
all_original_files_in_checkpoint = set(all_seen_hashes.values())

if all_original_files_in_checkpoint:
    # Iterate through all unique original files known from the checkpoint
    # Sort them for consistent order in the report
    for original_filename in sorted(list(all_original_files_in_checkpoint)): 
        # Check if this original file had duplicates moved in *this run*
        # Use .get() to return an empty list if no duplicates were moved for this original in this run
        duplicates_list_for_original = grouped_duplicates_this_run.get(original_filename, [])
        duplicates_str = ", ".join(duplicates_list_for_original)
        count = len(duplicates_list_for_original)
        
        comprehensive_report_data.append([original_filename, duplicates_str, count])

    # Create the DataFrame from the comprehensive data
    df_map = pd.DataFrame(comprehensive_report_data, columns=["Original File", "Duplicate Files (Moved)", "Duplicate Count"])
    
    # Sort the DataFrame: first by duplicate count (descending), then by original filename (ascending)
    df_map = df_map.sort_values(by=["Duplicate Count", "Original File"], ascending=[False, True])
    
    try:
        df_map.to_excel(comprehensive_duplicate_map_output, index=False)
        print(f"📝 Comprehensive duplicate mapping saved to: {comprehensive_duplicate_map_output}")
    except Exception as e:
        print(f"ERROR saving Excel report: {e}")
else:
    print("No unique files found in the checkpoint to generate a comprehensive report.")

# === Summary ===
print("\n--- SCAN COMPLETE ---")
print(f"Unique PDFs processed this run: {new_files_count}")
print(f"Duplicates moved to '{duplicate_folder}': {len(duplicates_found_this_run)}")
print(f"Total unique files in checkpoint: {len(all_seen_hashes)}") # This should now match the rows in your new report
print("---------------------")