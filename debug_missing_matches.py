cd ~/defi_protocol_security

python - <<'PY'
import csv, json, re
from datetime import datetime

def norm(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def d_defi(s):
    s = (s or "").strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def d_iso(s):
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
    except ValueError:
        return None

extra = json.load(open("hacks_extra.json"))
extra_by = {}
for e in extra:
    n = norm(e.get("protocol",""))
    dt = d_iso(e.get("date"))
    if n and dt:
        extra_by.setdefault(n, []).append((dt, e))

missing = []
with open("merged_hack_events.csv", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        name = row.get("name","")
        n = norm(name)
        dd = d_defi(row.get("exploit_date"))
        attached = (row.get("extra_source") or "").strip() != ""
        if dd and (not attached) and n in extra_by:
            cands = [(abs((dd - ed).days), ed, ev) for ed, ev in extra_by[n] if abs((dd - ed).days) <= 14]
            if cands:
                cands.sort(key=lambda x: x[0])
                best = cands[0]
                missing.append((name, row.get("exploit_date"), best[1], best[0], best[2].get("source")))

print("Missing-but-should-match:", len(missing))
for name, dstr, ed, delta, src in missing[:25]:
    print(f"{name:30s} defillama={dstr:10s} extra={ed}  Δ={delta:2d}d  src={src}")
PY
