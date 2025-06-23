import os
import hashlib
import json

# === CONFIG ===
input_pdf_folder = r"C:\Users\Surya.Pandidhar\Desktop\downloaded_pdfs"  # <-- Change if needed
output_hash_file = os.path.join(input_pdf_folder, "pdf_md5_hashes.json")


def get_md5_hash(file_path, chunk_size=4096):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    hashes = {}
    for file in os.listdir(input_pdf_folder):
        if not file.lower().endswith(".pdf"):
            continue
        full_path = os.path.join(input_pdf_folder, file)
        try:
            md5_hash = get_md5_hash(full_path)
            hashes[file] = md5_hash
            print(f"✅ {file} → {md5_hash}")
        except Exception as e:
            print(f"❌ Error reading {file}: {e}")

    # Save the hashes to JSON
    with open(output_hash_file, "w") as f:
        json.dump(hashes, f, indent=2)

    print(f"\n📁 Hashes saved to: {output_hash_file}")
    print(f"📊 Total PDFs processed: {len(hashes)}")


if __name__ == "__main__":
    main()
