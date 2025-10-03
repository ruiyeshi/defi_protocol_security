import os

EXPECTED_PATHS = [
    "data_raw/contracts",
    "data_raw/contracts/audit_metadata_certik.csv",
    "data_raw/contracts/verified_contracts.csv",
    "data_raw/contracts/verified_with_controls.csv",
    "data_raw/contracts/master_contracts.csv",
    "data_raw/contracts/protocols_seed.csv",
    "fetch_audit_certik_fallback.py",
    "fetch_controls_defillama.py",
    "fetch_audit_defisafety.py"
]

print("🧩 Sanity Check — Project Folder Structure\n")

base = os.path.expanduser("~/defi_protocol_security")
missing = []
for path in EXPECTED_PATHS:
    full_path = os.path.join(base, path)
    if not os.path.exists(full_path):
        print(f"❌ Missing → {path}")
        missing.append(path)
    else:
        size = os.path.getsize(full_path)
        print(f"✅ Found → {path} ({size:,} bytes)")

if not missing:
    print("\n🎯 All key files and folders are present and correctly structured.")
else:
    print(f"\n⚠️ Missing {len(missing)} required items. Please verify them manually.")