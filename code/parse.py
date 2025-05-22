
# -------- Parse BPG string --------
def parse_bpg(bpg_str):
    parts = bpg_str.split("~")
    return {
        "BIN": None if parts[0] == "NULL" else parts[0],
        "PCN": None if parts[1] == "NULL" else parts[1],
        "GroupID": None if parts[2] == "NULL" else parts[2],
    }

# -------- Parse LLM Output --------
def parse_llm_output(output):
    rows = []
    for line in output.splitlines():
        if "|" in line:
            parts = [p.strip() for p in line.split("|")]
            while len(parts) < 5:
                parts.append("")  # Fill missing fields
            parts.append("")  # PDF link
            rows.append(parts)
    return rows