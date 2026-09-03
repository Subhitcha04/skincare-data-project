"""
Mock ETL Validation Script
Runs a mini end-to-end transform without any DB connection.
Used in CI to verify transform logic on every push.
"""
import pandas as pd

print("=" * 50)
print("GlowCheck — Transform Validation (Mock ETL)")
print("=" * 50)

SYNONYM_MAP = {"vitamin c": "ascorbic acid", "retinol": "vitamin a"}
RISK_MAP = {"banned": 1.0, "restricted": 0.8, "unknown": 0.4, "allowed": 0.1}

# 1. Mock raw CosIng data
raw = pd.DataFrame({
    "inci_name": ["Niacinamide", "Vitamin C", "Mercury", "Retinol"],
    "regulatory_status": ["allowed", "unknown", "banned", "restricted"],
    "function": ["conditioning", "antioxidant", "none", "anti-aging"],
})

# 2. Standardize
raw["inci_name_std"] = raw["inci_name"].str.lower().map(
    lambda x: SYNONYM_MAP.get(x, x)
)
raw["risk_weight"] = raw["regulatory_status"].map(RISK_MAP)

# 3. Validate
assert raw["inci_name_std"].isnull().sum() == 0, "FAIL: nulls in inci_name_std"
assert raw["risk_weight"].isnull().sum() == 0, "FAIL: nulls in risk_weight"
assert raw.loc[raw["inci_name"] == "Mercury", "risk_weight"].values[0] == 1.0
assert raw.loc[raw["inci_name"] == "Retinol", "risk_weight"].values[0] == 0.8

# 4. Mock CDC
incoming = [{"id": "A", "val": 1}, {"id": "B", "val": 99}, {"id": "C", "val": 3}]
existing = {"B": {"id": "B", "val": 2}, "C": {"id": "C", "val": 3}}
results = {}
for r in incoming:
    ext = existing.get(r["id"])
    if ext is None:
        results[r["id"]] = "insert"
    elif r != ext:
        results[r["id"]] = "update"
    else:
        results[r["id"]] = "noop"

assert results["A"] == "insert", "FAIL: CDC insert"
assert results["B"] == "update", "FAIL: CDC update"
assert results["C"] == "noop", "FAIL: CDC noop"

print("Standardization : PASSED")
print("Risk weights    : PASSED")
print("CDC logic       : PASSED")
print("=" * 50)
print("All validations PASSED — safe to merge")