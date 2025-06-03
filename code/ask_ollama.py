import subprocess

# -------- Ask Ollama to Extract Info --------
def ask_ollama(text, known_bin, known_pcn, known_group):
    prompt = f"""
You are a data extraction assistant. From the PDF text below, identify sets of related information for pharmacy benefit plans.

Extract the following fields when available:
- BIN
- PCN
- Group ID
- Plan Type

Use the known information below as anchors to locate related values:
- Known BIN: {known_bin or "N/A"}
- Known PCN: {known_pcn or "N/A"}
- Known Group ID: {known_group or "N/A"}

Instructions:
1. If one or more of the known values appear in the text, extract the full set of related fields (BIN, PCN, Group ID, Plan Type) from the same section or context.
2. You can return multiple rows, but only include rows that are contextually linked to any of the known values.
3. If no related information is found, return one row with "N/A" in all fields and a comment: "No relevant match found".
4. Do not include explanations or output outside the table.

Output format:

BIN | PCN | Group ID | Plan Type | Comments

Text:
{text}
"""
    try:
        result = subprocess.run(
            ["ollama", "run", "llama3.1:8b"],
            input=prompt.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )
        if result.returncode != 0:
            print(f"Ollama error: {result.stderr.decode()}")
            return ""
        return result.stdout.decode()
    except Exception as e:
        print(f"Ollama call failed: {e}")
        return ""