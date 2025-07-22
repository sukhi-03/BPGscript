This repository contains a set of Python scripts designed to automate the process of deduplicating PDF files, extracting structured data from them using AI (Gemini) and PDF extraction (Camelot), and consolidating reports.

---

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Features](#2-features)
- [3. Project Structure](#3-project-structure)
- [4. Setup](#4-setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [API Key Configuration](#api-key-configuration)
  - [External Data Files](#external-data-files)
- [5. Configuration](#5-configuration)
- [6. How to Run the Scripts](#6-how-to-run-the-scripts)
  - [Execution Order](#execution-order)
  - [Step-by-Step Instructions](#step-by-step-instructions)
- [7. Script Details](#7-script-details)
- [8. Important Notes](#8-important-notes)

---

## 1. Introduction

This system streamlines the management and extraction of information from a large collection of PDF documents, specifically payer-related data. It handles duplicate detection, intelligent AI-powered extraction, and generates structured spreadsheets.



## 2. Features

- **PDF Deduplication**: Identifies and moves duplicate files using MD5 hashes.
- **Incremental Deduplication**: Uses checkpoint files for faster, resumable processing.
- **AI-Powered Extraction**: Uses Google Gemini to extract fields like Payer Name, BIN, PCN, GRP, Effective Date, etc.
- **Table Data Extraction**: Uses Camelot for parsing tables from PDFs.
- **PDF Splitting**: Automatically splits multipage PDFs for focused processing.
- **Resumable Workflow**: Checkpoints ensure safe script interruption/resumption.
- **Comprehensive Reports**: Excel outputs:
  - Grouped duplicate list
  - Extracted payer data
  - Consolidated original/duplicate/link mapping report


## 3. Project Structure
```java
¦   .env
¦   .gitignore
¦   environment.yml
¦   FolderStructure.txt
¦   
+---code
¦       DeDup.py
¦       gemini_camelot.py
¦       mastermapping.py
¦       pdfHashes.py
¦       PDFLinks.ipynb
¦       BPG_with_links.ipynb
¦       
+---input
¦       BPG.xlsx
¦       PayerProcessor.xlsx
¦       
+---ollama_script
¦   ¦   ask_ollama.py
¦   ¦   main.py
¦   ¦   parse.py
¦   ¦   pdf.py
¦   ¦   
¦   +---__pycache_
¦           ask_ollama.cpython-310.pyc
¦           parse.cpython-310.pyc
¦           pdf.cpython-310.pyc
¦                                 
+---QC
¦       payer_data_gemini_part2(200)postQC.xlsx
¦       pdf_93_QC.png
¦       PDF_94_qc.png
¦       
+---test
¦       BPGscript.ipynb
¦       check_column.ipynb
¦       PlanType_1.ipynb
¦       scrapper.py
¦       scrapperGemPost.py
¦       scrapper_gem.py
¦       
+---trial_pdfs
    ¦   pdf_10.pdf
    ¦   pdf_3.pdf
    ¦   pdf_65.pdf
    ¦   pdf_7.pdf            
```



## 4. Setup

### Prerequisites

- Python 3.8+ (Recommended: 3.9 or 3.10)
- [Ghostscript](https://www.ghostscript.com/download.html) (Required for Camelot)
  - Add Ghostscript's path to your system's `PATH` variable



### Installation

#### Clone the Repository

```bash
git clone <repository_url>
cd <repository_name>
```

#### Create and Activate a Conda Environment

Make sure you have Miniconda or Anaconda installed.
Then create the environment using the provided environment.yml file:

```bash
conda env create -f environment.yml
conda activate BPGscript
```
#### API Key Configuration
This project uses the Google Gemini API and Serper API. Follow the steps below:
1. Visit Google AI Studio.
2. Generate an API key.
3. Create a .env file in the project root directory.
4. Add the following line to it:

Serper API: Visit Serper.dev and generate your Serper API key.

```bash
gemini_api="YOUR_GEMINI_API_KEY_HERE"
serper_api="YOUR_SERPER_API_KEY_HERE"
```

### External Data Files

#### `PayerProcessor.xlsx` (required by `gemini_camelot.py`)

- **Path**: `input/PayerProcessor.xlsx`
- **Required Columns**: `Processor`, `Payer Parent`, `Payer`

#### `referral_links_first500.xlsx` (used by `mastermapping.py`)

- **Path**: `downloaded_pdfs/referral_links_first500.xlsx`
- **Required Columns**: `File Name`, `PDF LINK`



## 5. Configuration

Update the following paths in each script to match your system:

### `DeDup.py`
```python
BASE_FOLDER = r"D:\Projects\BPGscript\vol2(first200)"
```

### `gemini_camelot.py`
```python
mapping_path = r"D:\Projects\BPGscript\input\PayerProcessor.xlsx"
input_pdf_folder = r"D:\Projects\BPGscript\vol2(first200)"
output_folder = r"D:\Projects\BPGscript\output"
```

### `mastermapping.py`
```python
BASE_FOLDER = r"D:\Projects\BPGscript\downloaded_pdfs"
# Ensure duplicate_map_grouped.xlsx is accessible here or adjust path
```

## 6. How to Run the Scripts
Execution Order
1. DeDup.py → Detect duplicates

2. gemini_camelot.py → Extract data from PDFs

3. mastermapping.py → Consolidate mapping with external links

## 7. Script Details

### `bpg_pdf_search.ipynb`
-Purpose: ipynb script to search for BPG combinations and find payersheet PDFs on the internet.

### `DeDup.py`
- Purpose: Find and manage duplicate PDF files. using md5 algorithm (generates a hash key for every PDF from a binary level and remoe duplicates.)

- Input: PDFs from BASE_FOLDER

- Output:

    - duplicates/ folder

    - checkpoint_hashes.json

    - duplicate_map_grouped.xlsx

### `gemini_camelot.py`
- Purpose: Extract payer/plan details using Gemini AI and Camelot.

- Input:

    - PDFs from base folder (it will ignore the duplicates folder and only use the unique pdfs)

    - PayerProcessor.xlsx

    - .env with Gemini API key

- Output:

    - payer_data_volume_2(first200)_re.xlsx

    - checkpoint_processed_files.json

### `mastermapping.py`
- Purpose: Consolidate duplicate mapping with external links.

- Input:

    - duplicate_map_grouped.xlsx

    - referral_links_first500.xlsx

- Output:

    - consolidated_duplicate_report.xlsx

## 8. Important Notes
- ✅ Paths: Double-check paths in all scripts before running.

- ⚠️ Gemini API Quotas: Large volumes may trigger quota limits or usage costs.

- 🧪 Error Handling: Scripts include basic try/except, but malformed PDFs may still cause issues.

- 🐍 Python Compatibility: Recommended versions: Python 3.8–3.10.

- 📦 Ghostscript: Required by Camelot. Ensure it's installed and available in the system's PATH.
