"""Tests for feature dependency ordering (two-pass feature computation)."""

from bq_entity_resolution.features.engine import FeatureEngine


def test_split_feature_passes_all_independent():
    """Features with no inter-dependencies all go to pass 1."""
    features = [
        {"name": "name_clean", "expression": "UPPER(name)", "inputs": ["name"]},
        {"name": "phone_std", "expression": "REPLACE(phone, '-', '')", "inputs": ["phone"]},
    ]
    pass1, pass2 = FeatureEngine._split_feature_passes(features)
    assert len(pass1) == 2
    assert len(pass2) == 0


def test_split_feature_passes_detects_dependency():
    """Feature depending on another feature goes to pass 2."""
    features = [
        {"name": "name_clean", "expression": "UPPER(name)", "inputs": ["name"]},
        {"name": "name_soundex", "expression": "SOUNDEX(name_clean)", "inputs": ["name_clean"]},
    ]
    pass1, pass2 = FeatureEngine._split_feature_passes(features)
    assert len(pass1) == 1
    assert pass1[0]["name"] == "name_clean"
    assert len(pass2) == 1
    assert pass2[0]["name"] == "name_soundex"


def test_split_feature_passes_mixed():
    """Mix of independent and dependent features are sorted correctly."""
    features = [
        {"name": "a", "expression": "UPPER(col1)", "inputs": ["col1"]},
        {"name": "b", "expression": "LOWER(a)", "inputs": ["a"]},
        {"name": "c", "expression": "TRIM(col2)", "inputs": ["col2"]},
        {"name": "d", "expression": "CONCAT(a, c)", "inputs": ["a", "c"]},
    ]
    pass1, pass2 = FeatureEngine._split_feature_passes(features)
    pass1_names = {f["name"] for f in pass1}
    pass2_names = {f["name"] for f in pass2}
    assert pass1_names == {"a", "c"}
    assert pass2_names == {"b", "d"}


def test_split_feature_passes_empty_inputs():
    """Features with empty inputs (custom SQL) go to pass 1."""
    features = [
        {"name": "custom", "expression": "1 + 1", "inputs": []},
    ]
    pass1, pass2 = FeatureEngine._split_feature_passes(features)
    assert len(pass1) == 1
    assert len(pass2) == 0


def test_feature_template_has_two_passes(sample_config):
    """Feature SQL includes both features_pass1 and featured CTEs."""
    engine = FeatureEngine(sample_config)
    sql = engine.generate_feature_sql()
    assert "features_pass1" in sql
    assert "featured" in sql


def test_feature_template_dependent_features_rendered(sample_config):
    """When dependent_features is empty, template still renders correctly."""
    engine = FeatureEngine(sample_config)
    sql = engine.generate_feature_sql()
    # The template should have FROM features_pass1 p in the featured CTE
    assert "FROM features_pass1 p" in sql
