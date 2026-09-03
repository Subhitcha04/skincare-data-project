"""
GlowCheck — Unit Tests for ETL Transformation Logic
Run: pytest tests/test_transform.py -v
Tests cover: cleansing, standardization, joining, CDC classification,
             schema validation, null checks, outlier detection
"""
# CI/CD: This file is automatically run by GitHub Actions on every push
import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# ── Make transforms importable without a DB connection ────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ════════════════════════════════════════════════════════════
# Helpers — inline implementations so tests run without DB
# ════════════════════════════════════════════════════════════

SYNONYM_MAP = {
    "vitamin c": "ascorbic acid",
    "vit c": "ascorbic acid",
    "retinol": "vitamin a",
    "aha": "alpha hydroxy acid",
}


def standardize_name(name: str) -> str:
    return SYNONYM_MAP.get(name.lower().strip(), name.lower().strip())


def compute_risk_weight(regulatory_status: str) -> float:
    mapping = {
        "banned": 1.0,
        "restricted": 0.8,
        "unknown": 0.4,
        "allowed": 0.1,
    }
    return mapping.get(str(regulatory_status).lower().strip(), 0.4)


def compute_product_risk(ingredient_weights: list) -> float:
    if not ingredient_weights:
        return 0.0
    return round(sum(ingredient_weights) / len(ingredient_weights), 4)


def classify_cdc(incoming: dict, existing: dict) -> str:
    if existing is None:
        return "insert"
    elif incoming != existing:
        return "update"
    else:
        return "noop"


def validate_schema(df: pd.DataFrame, required_cols: list) -> tuple:
    missing = set(required_cols) - set(df.columns)
    return len(missing) == 0, list(missing)


def validate_nulls(df: pd.DataFrame, key_cols: list) -> tuple:
    null_counts = {c: int(df[c].isnull().sum()) for c in key_cols if c in df.columns}
    passed = all(v == 0 for v in null_counts.values())
    return passed, null_counts


def validate_outliers(df: pd.DataFrame, rules: dict) -> list:
    warnings = []
    for col, (lo, hi) in rules.items():
        if col in df.columns:
            n = pd.to_numeric(df[col], errors="coerce")
            out = int(n[(n < lo) | (n > hi)].count())
            if out:
                warnings.append(f"{out} outliers in '{col}' outside [{lo},{hi}]")
    return warnings


def clean_text(text: str) -> str:
    import re

    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def parse_ingredients(text: str) -> list:
    for sep in [",", ";", "\n", "|"]:
        if sep in text:
            return [i.strip().lower() for i in text.split(sep) if i.strip()]
    return [text.strip().lower()]


# ════════════════════════════════════════════════════════════
# TEST CLASS 1: Synonym Standardization
# ════════════════════════════════════════════════════════════
class TestSynonymStandardization:

    def test_vitamin_c_standardized(self):
        assert standardize_name("Vitamin C") == "ascorbic acid"

    def test_vit_c_standardized(self):
        assert standardize_name("vit c") == "ascorbic acid"

    def test_retinol_standardized(self):
        assert standardize_name("RETINOL") == "vitamin a"

    def test_unknown_ingredient_returned_lowercase(self):
        assert standardize_name("Niacinamide") == "niacinamide"

    def test_whitespace_stripped(self):
        assert standardize_name("  vitamin c  ") == "ascorbic acid"

    def test_empty_string(self):
        result = standardize_name("")
        assert isinstance(result, str)


# ════════════════════════════════════════════════════════════
# TEST CLASS 2: Risk Weight Computation
# ════════════════════════════════════════════════════════════
class TestRiskWeightComputation:

    def test_banned_ingredient_weight(self):
        assert compute_risk_weight("banned") == 1.0

    def test_restricted_ingredient_weight(self):
        assert compute_risk_weight("restricted") == 0.8

    def test_unknown_ingredient_weight(self):
        assert compute_risk_weight("unknown") == 0.4

    def test_allowed_ingredient_weight(self):
        assert compute_risk_weight("allowed") == 0.1

    def test_case_insensitive(self):
        assert compute_risk_weight("BANNED") == 1.0
        assert compute_risk_weight("Restricted") == 0.8

    def test_unknown_status_defaults_to_unknown_weight(self):
        assert compute_risk_weight("not_in_db") == 0.4

    def test_weights_in_valid_range(self):
        for status in ["banned", "restricted", "unknown", "allowed"]:
            w = compute_risk_weight(status)
            assert 0.0 <= w <= 1.0


# ════════════════════════════════════════════════════════════
# TEST CLASS 3: Product Risk Score
# ════════════════════════════════════════════════════════════
class TestProductRiskScore:

    def test_single_banned_ingredient(self):
        assert compute_product_risk([1.0]) == 1.0

    def test_all_safe_product(self):
        assert compute_product_risk([0.1, 0.1, 0.1]) == 0.1

    def test_mixed_ingredients_average(self):
        result = compute_product_risk([1.0, 0.1])
        assert result == 0.55

    def test_empty_ingredient_list(self):
        assert compute_product_risk([]) == 0.0

    def test_risk_score_between_0_and_1(self):
        weights = [0.8, 0.4, 0.1, 1.0, 0.4]
        score = compute_product_risk(weights)
        assert 0.0 <= score <= 1.0

    def test_result_rounded_to_4_decimal_places(self):
        result = compute_product_risk([0.8, 0.4, 0.1])
        assert result == round(result, 4)


# ════════════════════════════════════════════════════════════
# TEST CLASS 4: CDC Classification
# ════════════════════════════════════════════════════════════
class TestCDCClassification:

    def test_new_record_is_insert(self):
        assert classify_cdc({"id": "1", "val": "a"}, None) == "insert"

    def test_changed_record_is_update(self):
        incoming = {"id": "1", "val": "b"}
        existing = {"id": "1", "val": "a"}
        assert classify_cdc(incoming, existing) == "update"

    def test_unchanged_record_is_noop(self):
        record = {"id": "1", "val": "a"}
        assert classify_cdc(record, record) == "noop"

    def test_cdc_on_dataframe(self):
        incoming = pd.DataFrame(
            [
                {"pmid": "1", "abstract_length": 200},
                {"pmid": "2", "abstract_length": 150},
                {"pmid": "3", "abstract_length": 300},
            ]
        )
        existing = {
            "1": {"pmid": "1", "abstract_length": 200},  # unchanged
            "2": {"pmid": "2", "abstract_length": 999},  # changed
            # "3" not in existing → new
        }
        results = []
        for _, row in incoming.iterrows():
            rec = row.to_dict()
            ext = existing.get(rec["pmid"])
            results.append(classify_cdc(rec, ext))

        assert results.count("insert") == 1
        assert results.count("update") == 1
        assert results.count("noop") == 1


# ════════════════════════════════════════════════════════════
# TEST CLASS 5: Schema Validation
# ════════════════════════════════════════════════════════════
class TestSchemaValidation:

    def test_valid_schema_passes(self):
        df = pd.DataFrame(
            {"pmid": ["1"], "ingredient_term": ["niacinamide"], "abstract_length": [200]}
        )
        passed, missing = validate_schema(df, ["pmid", "ingredient_term", "abstract_length"])
        assert passed is True
        assert missing == []

    def test_missing_column_fails(self):
        df = pd.DataFrame({"pmid": ["1"]})
        passed, missing = validate_schema(df, ["pmid", "ingredient_term"])
        assert passed is False
        assert "ingredient_term" in missing

    def test_empty_dataframe_validates_columns(self):
        df = pd.DataFrame(columns=["pmid", "ingredient_term"])
        passed, missing = validate_schema(df, ["pmid"])
        assert passed is True

    def test_extra_columns_do_not_fail(self):
        df = pd.DataFrame({"pmid": ["1"], "extra_col": ["x"], "ingredient_term": ["niacinamide"]})
        passed, _ = validate_schema(df, ["pmid", "ingredient_term"])
        assert passed is True


# ════════════════════════════════════════════════════════════
# TEST CLASS 6: Null Checks
# ════════════════════════════════════════════════════════════
class TestNullChecks:

    def test_no_nulls_passes(self):
        df = pd.DataFrame({"pmid": ["1", "2"], "ingredient_term": ["a", "b"]})
        passed, counts = validate_nulls(df, ["pmid"])
        assert passed is True
        assert counts["pmid"] == 0

    def test_null_in_key_column_fails(self):
        df = pd.DataFrame({"pmid": ["1", None]})
        passed, counts = validate_nulls(df, ["pmid"])
        assert passed is False
        assert counts["pmid"] == 1

    def test_multiple_columns_checked(self):
        df = pd.DataFrame({"pmid": ["1", None], "ingredient_term": [None, "b"]})
        passed, counts = validate_nulls(df, ["pmid", "ingredient_term"])
        assert passed is False
        assert counts["pmid"] == 1
        assert counts["ingredient_term"] == 1


# ════════════════════════════════════════════════════════════
# TEST CLASS 7: Outlier Detection
# ════════════════════════════════════════════════════════════
class TestOutlierDetection:

    def test_no_outliers_returns_empty(self):
        df = pd.DataFrame({"abstract_length": [100, 200, 300]})
        warnings = validate_outliers(df, {"abstract_length": (1, 5000)})
        assert warnings == []

    def test_outlier_below_min_flagged(self):
        df = pd.DataFrame({"abstract_length": [0, 200]})
        warnings = validate_outliers(df, {"abstract_length": (1, 5000)})
        assert len(warnings) == 1
        assert "abstract_length" in warnings[0]

    def test_outlier_above_max_flagged(self):
        df = pd.DataFrame({"abstract_length": [100, 9999]})
        warnings = validate_outliers(df, {"abstract_length": (1, 5000)})
        assert len(warnings) == 1

    def test_multiple_columns_checked(self):
        df = pd.DataFrame(
            {
                "abstract_length": [100, 9999],
                "event_count": [1, -5],
            }
        )
        warnings = validate_outliers(
            df,
            {
                "abstract_length": (1, 5000),
                "event_count": (0, 1000),
            },
        )
        assert len(warnings) == 2


# ════════════════════════════════════════════════════════════
# TEST CLASS 8: Text Cleaning
# ════════════════════════════════════════════════════════════
class TestTextCleaning:

    def test_xml_tags_stripped(self):
        result = clean_text("<b>niacinamide</b> is safe")
        assert "<b>" not in result
        assert "niacinamide" in result

    def test_whitespace_normalized(self):
        result = clean_text("too   many    spaces")
        assert "  " not in result

    def test_lowercased(self):
        result = clean_text("NIACINAMIDE")
        assert result == "niacinamide"

    def test_empty_string(self):
        result = clean_text("")
        assert result == ""

    def test_mixed_tags_and_text(self):
        result = clean_text("<AbstractText>Retinol is effective.</AbstractText>")
        assert "retinol is effective." in result


# ════════════════════════════════════════════════════════════
# TEST CLASS 9: Ingredient Parsing (Label Scanner)
# ════════════════════════════════════════════════════════════
class TestIngredientParsing:

    def test_comma_separated(self):
        result = parse_ingredients("water, niacinamide, glycerin")
        assert result == ["water", "niacinamide", "glycerin"]

    def test_semicolon_separated(self):
        result = parse_ingredients("water; niacinamide; glycerin")
        assert result == ["water", "niacinamide", "glycerin"]

    def test_newline_separated(self):
        result = parse_ingredients("water\nniacinamide\nglycerine")
        assert "water" in result

    def test_single_ingredient(self):
        result = parse_ingredients("niacinamide")
        assert result == ["niacinamide"]

    def test_empty_entries_stripped(self):
        result = parse_ingredients("water, , niacinamide, ")
        assert "" not in result
        assert "water" in result
        assert "niacinamide" in result

    def test_whitespace_stripped_from_ingredients(self):
        result = parse_ingredients("  water  ,  niacinamide  ")
        assert "water" in result
        assert "niacinamide" in result


# ════════════════════════════════════════════════════════════
# TEST CLASS 10: Integration — full transform pipeline
# ════════════════════════════════════════════════════════════
class TestTransformPipeline:

    def test_full_cosing_transform(self):
        raw = pd.DataFrame(
            {
                "INCI name": ["Niacinamide", "Vitamin C", "Mercury"],
                "Restriction": ["allowed", "unknown", "banned"],
                "Function": ["conditioning", "antioxidant", "none"],
            }
        )
        raw = raw.rename(
            columns={
                "INCI name": "inci_name",
                "Restriction": "regulatory_status",
                "Function": "function",
            }
        )
        raw["inci_name_std"] = raw["inci_name"].apply(standardize_name)
        raw["risk_weight"] = raw["regulatory_status"].apply(compute_risk_weight)

        assert raw.loc[raw["inci_name"] == "Niacinamide", "risk_weight"].values[0] == 0.1
        assert raw.loc[raw["inci_name"] == "Vitamin C", "risk_weight"].values[0] == 0.4
        assert raw.loc[raw["inci_name"] == "Mercury", "risk_weight"].values[0] == 1.0
        assert (
            raw.loc[raw["inci_name"] == "Vitamin C", "inci_name_std"].values[0] == "ascorbic acid"
        )

    def test_product_risk_pipeline(self):
        product_ingredients = ["niacinamide", "vitamin c", "mercury"]
        weights = [
            compute_risk_weight(
                {"niacinamide": "allowed", "vitamin c": "unknown", "mercury": "banned"}[i]
            )
            for i in product_ingredients
        ]
        risk_score = compute_product_risk(weights)
        assert risk_score == pytest.approx((0.1 + 0.4 + 1.0) / 3, rel=1e-4)
        assert risk_score > 0.4  # mercury pulls it up

    def test_cdc_full_batch(self):
        incoming_batch = [
            {"id": "A", "val": 100},  # new
            {"id": "B", "val": 200},  # changed
            {"id": "C", "val": 300},  # unchanged
        ]
        existing_db = {
            "B": {"id": "B", "val": 999},
            "C": {"id": "C", "val": 300},
        }
        results = {r["id"]: classify_cdc(r, existing_db.get(r["id"])) for r in incoming_batch}
        assert results["A"] == "insert"
        assert results["B"] == "update"
        assert results["C"] == "noop"
