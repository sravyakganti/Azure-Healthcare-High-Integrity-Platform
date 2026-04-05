"""
tests/test_transformations.py
------------------------------
Unit tests for processing/bronze_to_silver.py (SilverTransformer).

Covers:
* transform_patients: dob parsing, full_name derivation, age_group assignment
* transform_encounters: visit_type standardisation, is_readmitted boolean
* transform_lab_tests: is_abnormal boolean
* transform_claims: payment_rate computation, zero-division guard
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make the processing package importable from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "processing"))

from bronze_to_silver import SilverTransformer  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def transformer() -> SilverTransformer:
    return SilverTransformer()


# ---------------------------------------------------------------------------
# transform_patients
# ---------------------------------------------------------------------------

class TestTransformPatients:

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003", "P004"],
                "first_name": ["Alice", "Bob", "Carol", "Dan"],
                "last_name": ["Smith", "Jones", "White", "Brown"],
                "dob": ["14-05-1990", "01-01-1940", "20-11-2010", "05-03-1955"],
                "age": [33, 83, 13, 68],
                "gender": ["female", "Male", "FEMALE", "unknown"],
                "insurance_type": ["Medicare", "Medicaid", "Private", "Medicare"],
                "registration_date": ["01-01-2020", "15-06-2018", "10-10-2021", "22-02-2019"],
                "phone": ["555-0100", None, "", "555-0199"],
                "email": [None, "bob@example.com", "", "dan@example.com"],
            }
        )

    def test_dob_parsed_to_datetime(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert pd.api.types.is_datetime64_any_dtype(df["dob"]), \
            "dob column must be parsed to datetime dtype."

    def test_dob_correct_value(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        expected = pd.Timestamp("1990-05-14")
        assert df.loc[0, "dob"] == expected

    def test_registration_date_parsed(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert pd.api.types.is_datetime64_any_dtype(df["registration_date"])

    def test_full_name_created(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert "full_name" in df.columns
        assert df.loc[0, "full_name"] == "Alice Smith"
        assert df.loc[1, "full_name"] == "Bob Jones"

    def test_age_group_pediatric(self, transformer: SilverTransformer) -> None:
        """Age 13 → Pediatric."""
        df = transformer.transform_patients(self._make_df())
        row = df[df["patient_id"] == "P003"].iloc[0]
        assert row["age_group"] == "Pediatric"

    def test_age_group_adult(self, transformer: SilverTransformer) -> None:
        """Age 33 → Adult."""
        df = transformer.transform_patients(self._make_df())
        row = df[df["patient_id"] == "P001"].iloc[0]
        assert row["age_group"] == "Adult"

    def test_age_group_senior(self, transformer: SilverTransformer) -> None:
        """Age 83 → Senior."""
        df = transformer.transform_patients(self._make_df())
        row = df[df["patient_id"] == "P002"].iloc[0]
        assert row["age_group"] == "Senior"

    def test_age_boundary_65_is_senior(self, transformer: SilverTransformer) -> None:
        """Age 65 is on the boundary → Senior."""
        df = self._make_df()
        df.loc[0, "age"] = 65
        result = transformer.transform_patients(df)
        assert result.loc[0, "age_group"] == "Senior"

    def test_age_boundary_18_is_adult(self, transformer: SilverTransformer) -> None:
        """Age 18 → Adult."""
        df = self._make_df()
        df.loc[0, "age"] = 18
        result = transformer.transform_patients(df)
        assert result.loc[0, "age_group"] == "Adult"

    def test_gender_standardised_to_female(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert df.loc[0, "gender"] == "Female"
        assert df.loc[2, "gender"] == "Female"

    def test_gender_unknown_preserved(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert df.loc[3, "gender"] == "Unknown"

    def test_missing_phone_filled(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert df.loc[1, "phone"] == "Unknown"

    def test_empty_phone_filled(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert df.loc[2, "phone"] == "Unknown"

    def test_missing_email_filled(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert df.loc[0, "email"] == "Unknown"

    def test_silver_processed_at_added(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_patients(self._make_df())
        assert "silver_processed_at" in df.columns


# ---------------------------------------------------------------------------
# transform_encounters
# ---------------------------------------------------------------------------

class TestTransformEncounters:

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "encounter_id": ["E001", "E002", "E003", "E004"],
                "patient_id": ["P001", "P002", "P003", "P004"],
                "visit_date": ["01-06-2024", "15-03-2024", "22-11-2023", "10-01-2024"],
                "discharge_date": ["02-06-2024", "20-03-2024", None, None],
                "visit_type": ["Inpatients", "Outpatient", "Emergency", "Telehealth"],
                "department": ["Cardiology", "Neurology", "ER", "Primary Care"],
                "reason_for_visit": ["Chest pain", "Headache", "Fracture", "Cough"],
                "diagnosis_code": ["I21", "G43", "S52", "J06"],
                "admission_type": ["Emergency", None, "Emergency", None],
                "length_of_stay": [5, None, None, None],
                "status": ["Completed", "Completed", "Completed", "Completed"],
                "readmitted_flag": ["Yes", "No", "No", "Yes"],
            }
        )

    def test_visit_type_inpatients_mapped_to_inpatient(
        self, transformer: SilverTransformer
    ) -> None:
        """'Inpatients' must be normalised to 'Inpatient'."""
        df = transformer.transform_encounters(self._make_df())
        assert df.loc[0, "visit_type"] == "Inpatient"

    def test_valid_visit_types_preserved(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert df.loc[1, "visit_type"] == "Outpatient"
        assert df.loc[2, "visit_type"] == "Emergency"
        assert df.loc[3, "visit_type"] == "Telehealth"

    def test_is_readmitted_true_for_yes(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert df.loc[0, "is_readmitted"] is True or df.loc[0, "is_readmitted"] == True  # noqa: E712

    def test_is_readmitted_false_for_no(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert df.loc[1, "is_readmitted"] is False or df.loc[1, "is_readmitted"] == False  # noqa: E712

    def test_is_readmitted_dtype_is_bool(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert df["is_readmitted"].dtype == bool

    def test_outpatient_missing_los_filled_with_zero(
        self, transformer: SilverTransformer
    ) -> None:
        df = transformer.transform_encounters(self._make_df())
        # Row 1 is Outpatient with no length_of_stay → should be 0
        assert df.loc[1, "length_of_stay"] == 0

    def test_inpatient_missing_los_not_auto_filled(
        self, transformer: SilverTransformer
    ) -> None:
        """Inpatient rows should NOT have LOS auto-filled to 0."""
        raw = self._make_df()
        raw.loc[0, "length_of_stay"] = None
        raw.loc[0, "visit_type"] = "Inpatient"
        df = transformer.transform_encounters(raw)
        # Should remain NaN — we only fill ambulatory types
        assert pd.isna(df.loc[0, "length_of_stay"])

    def test_visit_date_parsed_to_datetime(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert pd.api.types.is_datetime64_any_dtype(df["visit_date"])

    def test_silver_processed_at_added(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_encounters(self._make_df())
        assert "silver_processed_at" in df.columns


# ---------------------------------------------------------------------------
# transform_lab_tests
# ---------------------------------------------------------------------------

class TestTransformLabTests:

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "lab_id": ["L001", "L002", "L003", "L004"],
                "encounter_id": ["E001", "E002", "E003", "E004"],
                "test_name": ["CBC", "BMP", "Lipid Panel", "HbA1c"],
                "test_code": ["85025", "80048", "80061", "83036"],
                "specimen_type": ["Blood", "Blood", "Blood", "Blood"],
                "test_result": ["Abnormal", "Normal", "abnormal", "Normal"],
                "units": ["cells/uL", "mg/dL", "mg/dL", "%"],
                "normal_range": ["4.5-11.0", "70-100", "<200", "4.0-5.6"],
                "test_date": ["01-06-2024", "15-03-2024", "22-11-2023", "10-01-2024"],
                "status": ["final", "preliminary", "CORRECTED", "cancelled"],
            }
        )

    def test_is_abnormal_true_when_abnormal(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert df.loc[0, "is_abnormal"] is True or df.loc[0, "is_abnormal"] == True  # noqa: E712

    def test_is_abnormal_false_when_normal(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert df.loc[1, "is_abnormal"] is False or df.loc[1, "is_abnormal"] == False  # noqa: E712

    def test_is_abnormal_case_insensitive(self, transformer: SilverTransformer) -> None:
        """Lowercase 'abnormal' must also set is_abnormal=True."""
        df = transformer.transform_lab_tests(self._make_df())
        assert df.loc[2, "is_abnormal"] is True or df.loc[2, "is_abnormal"] == True  # noqa: E712

    def test_is_abnormal_dtype_is_bool(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert df["is_abnormal"].dtype == bool

    def test_status_title_cased(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert df.loc[0, "status"] == "Final"
        assert df.loc[1, "status"] == "Preliminary"
        assert df.loc[2, "status"] == "Corrected"
        assert df.loc[3, "status"] == "Cancelled"

    def test_test_date_parsed_to_datetime(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert pd.api.types.is_datetime64_any_dtype(df["test_date"])

    def test_silver_processed_at_added(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_lab_tests(self._make_df())
        assert "silver_processed_at" in df.columns


# ---------------------------------------------------------------------------
# transform_claims
# ---------------------------------------------------------------------------

class TestTransformClaims:

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "billing_id": ["B001", "B002", "B003", "B004"],
                "patient_id": ["P001", "P002", "P003", "P004"],
                "encounter_id": ["E001", "E002", "E003", "E004"],
                "insurance_provider": ["Aetna", "BlueCross", "UHC", "Cigna"],
                "payment_method": ["EFT", "Check", "EFT", "EFT"],
                "claim_id": ["C001", "C002", "C003", "C004"],
                "claim_billing_date": [
                    "06-02-2025 00:00",
                    "15-03-2024 10:30",
                    "01-11-2023 08:00",
                    "20-01-2024 14:15",
                ],
                "billed_amount": [1500.0, 3200.0, 0.0, 800.0],
                "paid_amount": [1200.0, 2800.0, 0.0, 640.0],
                "claim_status": ["Paid", "Denied", "Pending", "Paid"],
                "denial_reason": [None, "Not Medically Necessary", None, None],
            }
        )

    def test_payment_rate_computed_correctly(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        # 1200 / 1500 = 0.8
        assert abs(df.loc[0, "payment_rate"] - 0.8) < 1e-6

    def test_payment_rate_zero_when_billed_is_zero(
        self, transformer: SilverTransformer
    ) -> None:
        """Division by zero must result in payment_rate = 0.0."""
        df = transformer.transform_claims(self._make_df())
        assert df.loc[2, "payment_rate"] == 0.0

    def test_payment_rate_another_row(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        # 640 / 800 = 0.8
        assert abs(df.loc[3, "payment_rate"] - 0.8) < 1e-6

    def test_is_denied_true_for_denied(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert df.loc[1, "is_denied"] is True or df.loc[1, "is_denied"] == True  # noqa: E712

    def test_is_denied_false_for_paid(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert df.loc[0, "is_denied"] is False or df.loc[0, "is_denied"] == False  # noqa: E712

    def test_denial_reason_filled_with_na(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        # Rows 0, 2, 3 had None denial_reason → should be "N/A"
        assert df.loc[0, "denial_reason"] == "N/A"
        assert df.loc[2, "denial_reason"] == "N/A"

    def test_denial_reason_preserved_when_set(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert df.loc[1, "denial_reason"] == "Not Medically Necessary"

    def test_claim_billing_date_parsed(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert pd.api.types.is_datetime64_any_dtype(df["claim_billing_date"])

    def test_claim_billing_date_correct_value(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        expected = pd.Timestamp("2025-02-06 00:00")
        assert df.loc[0, "claim_billing_date"] == expected

    def test_silver_processed_at_added(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert "silver_processed_at" in df.columns

    def test_billed_amount_numeric(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert pd.api.types.is_float_dtype(df["billed_amount"])

    def test_paid_amount_numeric(self, transformer: SilverTransformer) -> None:
        df = transformer.transform_claims(self._make_df())
        assert pd.api.types.is_float_dtype(df["paid_amount"])
