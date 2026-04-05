"""
tests/test_validators.py
------------------------
Unit tests for ingestion/validators.py

Covers:
* PatientSchema accepts valid data
* PatientSchema rejects negative age
* EncounterSchema accepts valid data
* ClaimsSchema rejects negative billed_amount
* validate_dataframe returns a correctly populated ValidationResult
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make the ingestion package importable from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))

from validators import (  # noqa: E402
    ClaimsSchema,
    EncounterSchema,
    LabTestSchema,
    PatientSchema,
    ValidationResult,
    validate_dataframe,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal valid DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture()
def valid_patients_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "patient_id": ["P001", "P002", "P003"],
            "first_name": ["Alice", "Bob", "Carol"],
            "last_name": ["Smith", "Jones", "White"],
            "dob": ["01-06-1990", "15-03-1975", "22-11-2000"],
            "age": [34, 49, 23],
            "gender": ["Female", "Male", "Female"],
            "insurance_type": ["Medicare", "Private", "Medicaid"],
            "registration_date": ["01-01-2020", "15-06-2018", "10-10-2021"],
        }
    )


@pytest.fixture()
def valid_encounters_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "encounter_id": ["E001", "E002", "E003"],
            "patient_id": ["P001", "P002", "P003"],
            "visit_date": ["01-06-2024", "15-03-2024", "22-11-2023"],
            "visit_type": ["Outpatient", "Inpatient", "Emergency"],
            "department": ["Cardiology", "Oncology", "ER"],
            "status": ["Completed", "Completed", "Completed"],
            "readmitted_flag": ["No", "Yes", "No"],
        }
    )


@pytest.fixture()
def valid_lab_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lab_id": ["L001", "L002", "L003"],
            "encounter_id": ["E001", "E002", "E003"],
            "test_name": ["CBC", "BMP", "Lipid Panel"],
            "test_date": ["01-06-2024", "15-03-2024", "22-11-2023"],
            "status": ["Final", "Preliminary", "Corrected"],
        }
    )


@pytest.fixture()
def valid_claims_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "billing_id": ["B001", "B002", "B003"],
            "patient_id": ["P001", "P002", "P003"],
            "encounter_id": ["E001", "E002", "E003"],
            "billed_amount": [1500.00, 3200.50, 750.25],
            "paid_amount": [1200.00, 2800.00, 600.00],
            "claim_status": ["Paid", "Denied", "Pending"],
        }
    )


# ---------------------------------------------------------------------------
# PatientSchema tests
# ---------------------------------------------------------------------------

class TestPatientSchema:
    def test_valid_patients_pass(self, valid_patients_df: pd.DataFrame) -> None:
        """PatientSchema must accept a well-formed patients DataFrame."""
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert result.is_valid is True
        assert result.row_count == 3
        assert result.valid_row_count == 3
        assert result.errors == []

    def test_negative_age_rejected(self, valid_patients_df: pd.DataFrame) -> None:
        """PatientSchema must flag rows where age < 0."""
        bad_df = valid_patients_df.copy()
        bad_df.loc[0, "age"] = -5  # invalid
        result = validate_dataframe(bad_df, PatientSchema, "PatientSchema")
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_age_over_120_rejected(self, valid_patients_df: pd.DataFrame) -> None:
        """PatientSchema must flag rows where age > 120."""
        bad_df = valid_patients_df.copy()
        bad_df.loc[1, "age"] = 150  # invalid
        result = validate_dataframe(bad_df, PatientSchema, "PatientSchema")
        assert result.is_valid is False

    def test_invalid_gender_rejected(self, valid_patients_df: pd.DataFrame) -> None:
        """PatientSchema must flag an unrecognised gender value."""
        bad_df = valid_patients_df.copy()
        bad_df.loc[2, "gender"] = "Robot"
        result = validate_dataframe(bad_df, PatientSchema, "PatientSchema")
        assert result.is_valid is False

    def test_row_count_matches(self, valid_patients_df: pd.DataFrame) -> None:
        """ValidationResult.row_count must equal len(df)."""
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert result.row_count == len(valid_patients_df)

    def test_schema_name_stored(self, valid_patients_df: pd.DataFrame) -> None:
        """ValidationResult must store the schema_name passed in."""
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert result.schema_name == "PatientSchema"


# ---------------------------------------------------------------------------
# EncounterSchema tests
# ---------------------------------------------------------------------------

class TestEncounterSchema:
    def test_valid_encounters_pass(self, valid_encounters_df: pd.DataFrame) -> None:
        """EncounterSchema must accept a well-formed encounters DataFrame."""
        result = validate_dataframe(valid_encounters_df, EncounterSchema, "EncounterSchema")
        assert result.is_valid is True
        assert result.valid_row_count == 3

    def test_inpatients_variant_accepted(self, valid_encounters_df: pd.DataFrame) -> None:
        """EncounterSchema must accept the 'Inpatients' variant of visit_type."""
        df = valid_encounters_df.copy()
        df.loc[0, "visit_type"] = "Inpatients"
        result = validate_dataframe(df, EncounterSchema, "EncounterSchema")
        assert result.is_valid is True

    def test_invalid_visit_type_rejected(self, valid_encounters_df: pd.DataFrame) -> None:
        """EncounterSchema must reject an unknown visit_type value."""
        bad_df = valid_encounters_df.copy()
        bad_df.loc[0, "visit_type"] = "WalkIn"
        result = validate_dataframe(bad_df, EncounterSchema, "EncounterSchema")
        assert result.is_valid is False

    def test_invalid_readmitted_flag_rejected(self, valid_encounters_df: pd.DataFrame) -> None:
        """EncounterSchema must reject readmitted_flag values other than Yes/No."""
        bad_df = valid_encounters_df.copy()
        bad_df.loc[1, "readmitted_flag"] = "Maybe"
        result = validate_dataframe(bad_df, EncounterSchema, "EncounterSchema")
        assert result.is_valid is False


# ---------------------------------------------------------------------------
# LabTestSchema tests
# ---------------------------------------------------------------------------

class TestLabTestSchema:
    def test_valid_lab_tests_pass(self, valid_lab_df: pd.DataFrame) -> None:
        """LabTestSchema must accept a well-formed lab_tests DataFrame."""
        result = validate_dataframe(valid_lab_df, LabTestSchema, "LabTestSchema")
        assert result.is_valid is True

    def test_invalid_status_rejected(self, valid_lab_df: pd.DataFrame) -> None:
        """LabTestSchema must reject a status value outside the allowed set."""
        bad_df = valid_lab_df.copy()
        bad_df.loc[0, "status"] = "Pending"
        result = validate_dataframe(bad_df, LabTestSchema, "LabTestSchema")
        assert result.is_valid is False


# ---------------------------------------------------------------------------
# ClaimsSchema tests
# ---------------------------------------------------------------------------

class TestClaimsSchema:
    def test_valid_claims_pass(self, valid_claims_df: pd.DataFrame) -> None:
        """ClaimsSchema must accept a well-formed claims DataFrame."""
        result = validate_dataframe(valid_claims_df, ClaimsSchema, "ClaimsSchema")
        assert result.is_valid is True

    def test_negative_billed_amount_rejected(self, valid_claims_df: pd.DataFrame) -> None:
        """ClaimsSchema must reject a negative billed_amount."""
        bad_df = valid_claims_df.copy()
        bad_df.loc[0, "billed_amount"] = -100.0
        result = validate_dataframe(bad_df, ClaimsSchema, "ClaimsSchema")
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_negative_paid_amount_rejected(self, valid_claims_df: pd.DataFrame) -> None:
        """ClaimsSchema must reject a negative paid_amount."""
        bad_df = valid_claims_df.copy()
        bad_df.loc[1, "paid_amount"] = -50.0
        result = validate_dataframe(bad_df, ClaimsSchema, "ClaimsSchema")
        assert result.is_valid is False

    def test_invalid_claim_status_rejected(self, valid_claims_df: pd.DataFrame) -> None:
        """ClaimsSchema must reject a claim_status outside the allowed set."""
        bad_df = valid_claims_df.copy()
        bad_df.loc[2, "claim_status"] = "Under Review"
        result = validate_dataframe(bad_df, ClaimsSchema, "ClaimsSchema")
        assert result.is_valid is False


# ---------------------------------------------------------------------------
# ValidationResult dataclass tests
# ---------------------------------------------------------------------------

class TestValidationResult:
    def test_is_valid_field_present(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert hasattr(result, "is_valid")

    def test_errors_field_is_list(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert isinstance(result.errors, list)

    def test_row_count_field_present(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert isinstance(result.row_count, int)

    def test_valid_row_count_field_present(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert isinstance(result.valid_row_count, int)

    def test_schema_name_field_present(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert isinstance(result.schema_name, str)

    def test_valid_row_count_leq_row_count(self, valid_patients_df: pd.DataFrame) -> None:
        bad_df = valid_patients_df.copy()
        bad_df.loc[0, "age"] = -1
        result = validate_dataframe(bad_df, PatientSchema, "PatientSchema")
        assert result.valid_row_count <= result.row_count

    def test_error_rate_on_clean_data(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        assert result.error_rate == 0.0

    def test_summary_dict_structure(self, valid_patients_df: pd.DataFrame) -> None:
        result = validate_dataframe(valid_patients_df, PatientSchema, "PatientSchema")
        summary = result.summary()
        for key in ("schema_name", "is_valid", "row_count", "valid_row_count",
                    "invalid_row_count", "error_rate", "error_count", "errors"):
            assert key in summary, f"Missing key in summary: {key}"
