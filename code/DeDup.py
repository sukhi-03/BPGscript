import os
import hashlib
import json
import shutil
import pandas as pd

# === CONFIGURATION ===
# Use a more robust way to define paths
BASE_FOLDER = r"D:\Projects\BPGscript\vol2(first200)"
input_pdf_folder = BASE_FOLDER # The main folder to scan
hash_checkpoint_path = os.path.join(BASE_FOLDER, "checkpoint_hashes.json")
duplicate_folder = os.path.join(BASE_FOLDER, "duplicates")
duplicate_map_output = os.path.join(BASE_FOLDER, "duplicate_map_grouped.xlsx") # <<< CHANGE: More descriptive output filename

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
    with open(filepath, 'rb') as f:
        while chunk := f.read(block_size):
            hasher.update(chunk)
    return hasher.hexdigest()

# === Scan and process PDFs ===
duplicates_found_this_run = []
new_files_count = 0
# <<< CHANGE: New data structure for better mapping >>>
# This will store data as: {'original_file.pdf': ['copy1.pdf', 'copy2.pdf']}
grouped_duplicates = {}


print("\nScanning for duplicate PDFs...")
# Iterate through all items in the folder
for filename in os.listdir(input_pdf_folder):
    filepath = os.path.join(input_pdf_folder, filename)

    # Skip directories (like 'duplicates') and non-PDF files
    if not os.path.isfile(filepath) or not filename.lower().endswith(".pdf"):
        continue

    # Calculate hash
    file_hash = get_file_hash(filepath)

    if file_hash in all_seen_hashes:
        # This is a duplicate (either from a past run or this run)
        original_filename = all_seen_hashes[file_hash]

        # Avoid flagging a file as a duplicate of itself if it's the original
        if filename == original_filename:
            continue

        print(f"⚠️  Duplicate detected: '{filename}' is a copy of '{original_filename}'")
        duplicates_found_this_run.append(filename)
        
        # <<< CHANGE: Populate our new grouped dictionary >>>
        # If the original file isn't a key yet, create an empty list for it
        grouped_duplicates.setdefault(original_filename, []).append(filename)
        
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
with open(hash_checkpoint_path, "w") as f:
    json.dump(all_seen_hashes, f, indent=4)

# <<< CHANGE: Reworked logic to save the improved map to Excel >>>
if grouped_duplicates:
    # Convert the grouped dictionary into a list of records for the DataFrame
    output_data = []
    for original, duplicates_list in grouped_duplicates.items():
        # Join the list of duplicates into a single string for the Excel cell
        duplicates_str = ", ".join(duplicates_list)
        count = len(duplicates_list)
        output_data.append([original, duplicates_str, count])
    
    # Create the DataFrame
    df_map = pd.DataFrame(output_data, columns=["Original File", "Duplicate Files (Moved)", "Duplicate Count"])
    
    # Sort the DataFrame by the number of duplicates to easily see the most copied files
    df_map = df_map.sort_values(by="Duplicate Count", ascending=False)
    
    df_map.to_excel(duplicate_map_output, index=False)
    print(f"📝 Improved duplicate mapping saved to: {duplicate_map_output}")
else:
    print("✅ No new duplicates were found in this run.")

# === Summary ===
print("\n--- SCAN COMPLETE ---")
print(f"Unique PDFs processed this run: {new_files_count}")
print(f"Duplicates moved to '{duplicate_folder}': {len(duplicates_found_this_run)}")
print(f"Total unique files in checkpoint: {len(all_seen_hashes)}")
print("---------------------")