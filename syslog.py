# syslog_ip_filter_minimal.py
from pathlib import Path
import re
import pandas as pd

DATA_DIR = Path(".")  # current directory

# --- Step 1: Find syslog file ---
syslog_file = None
for p in [DATA_DIR / "syslog.1.csv", DATA_DIR / "syslog.csv"]:
    if p.exists():
        syslog_file = p
        break
if not syslog_file:
    for f in DATA_DIR.iterdir():
        if "syslog" in f.name.lower():
            syslog_file = f
            break

if not syslog_file:
    raise FileNotFoundError("No syslog file found in the current directory.")

print(f"Using syslog file: {syslog_file}")

# --- Step 2: Load syslog data ---
df = pd.read_csv(syslog_file, dtype=str, keep_default_na=False)
text_cols = [c for c in df.columns if df[c].dtype == object or df[c].dtype == "string"]
if text_cols:
    df["message"] = df[text_cols].astype(str).agg(" ".join, axis=1)
else:
    df["message"] = df.astype(str).agg(" ".join, axis=1)

# --- Step 3: Extract only IPv4 addresses ---
ipv4_re = re.compile(r"\b((?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3})\b")
all_ips = []
for msg in df["message"]:
    all_ips.extend(ipv4_re.findall(str(msg)))

# Remove duplicates
unique_ips = sorted(set(all_ips))

# Save extracted IPs (only one column)
pd.DataFrame({"ip": unique_ips}).to_csv("extracted_ips.csv", index=False)

print(f"Extracted {len(unique_ips)} unique IPs.")

# --- Step 4: Match with Device_data (if present) ---
device_file = None
for f in DATA_DIR.iterdir():
    if "device" in f.name.lower():
        device_file = f
        break

if device_file:
    print(f"Using Device_data file: {device_file}")
    try:
        dev = pd.read_csv(device_file, dtype=str, keep_default_na=False)
    except Exception:
        dev = pd.read_excel(device_file, dtype=str)

    ip_col = next((c for c in dev.columns if "ip" in c.lower() or "address" in c.lower()), None)
    if ip_col:
        device_ips = set(dev[ip_col].astype(str).str.strip())
        matched = sorted(set(unique_ips) & device_ips)
        unmatched = sorted(set(unique_ips) - device_ips)
    else:
        print("No IP column found in Device_data.")
        matched = []
        unmatched = unique_ips
else:
    print("No Device_data file found. All IPs marked as unmatched.")
    matched = []
    unmatched = unique_ips

# --- Step 5: Save matched/unmatched lists ---
pd.DataFrame({"ip": matched}).to_csv("matched_ips.csv", index=False)
pd.DataFrame({"ip": unmatched}).to_csv("unmatched_ips.csv", index=False)

print("Done!")
print(f"- extracted_ips.csv : {len(unique_ips)} IPs")
print(f"- matched_ips.csv   : {len(matched)} IPs")
print(f"- unmatched_ips.csv : {len(unmatched)} IPs")
