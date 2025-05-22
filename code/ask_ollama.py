
# -------- Ask Ollama to Extract Info --------
def ask_ollama(text, expected_bin, expected_pcn, expected_group):
    prompt = f"""
You are a data extraction expert. From the following PDF content, extract any BIN, PCN, Group ID, and Plan Type info you find.

Then compare the extracted values to these expected inputs:

Expected BIN: {expected_bin or "N/A"}
Expected PCN: {expected_pcn or "N/A"}
Expected Group ID: {expected_group or "N/A"}

Only return rows that either match or closely resemble the expected values. If there's no match, write that in the Comments.

Return only a table with columns:

BIN | PCN | Group ID | Plan type | Comments

Text:
{text}
"""
    try:
        result = subprocess.run(
            ["ollama", "run", "llama3.1:8b-instruct-q2_K"],
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