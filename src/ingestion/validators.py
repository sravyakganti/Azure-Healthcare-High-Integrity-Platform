"""
ingestion/validators.py
-----------------------
Pandera schema definitions and validation helpers for each healthcare dataset.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from loguru import logger


# ---------------------------------------------------------------------------
# Pandera Schemas
# ---------------------------------------------------------------------------

PatientSchema = DataFrameSchema(
    columns={
        "patient_id": Column(
            str,
            nullable=False,
            checks=Check(lambda s: s.str.strip().str.len() > 0, element_wise=False,
                         error="patient_id must not be blank"),
        ),
        "first_name": Column(str, nullable=True),
        "last_name": Column(str, nullable=True),
        "dob": Column(str, nullable=True),
        "age": Column(
            int,
            nullable=True,
            checks=[
                Check.greater_than_or_equal_to(0),
                Check.less_than_or_equal_to(120),
            ],
        ),
        "gender": Column(
            str,
            nullable=True,
            checks=Check.isin(["Female", "Male", "Other", "Unknown"]),
        ),
        "insurance_type": Column(str, nullable=True),
        "registration_date": Column(str, nullable=True),
    },
    coerce=True,
    strict=False,  # allow extra columns present in the CSV
    name="PatientSchema",
)


EncounterSchema = DataFrameSchema(
    columns={
        "encounter_id": Column(str, nullable=False),
        "patient_id": Column(str, nullable=False),
        "visit_date": Column(str, nullable=True),
        "visit_type": Column(
            str,
            nullable=True,
            checks=Check.isin(
                ["Outpatient", "Inpatient", "Emergency", "Telehealth", "Inpatients"]
            ),
        ),
        "department": Column(str, nullable=True),
        "status": Column(str, nullable=True),
        "readmitted_flag": Column(
            str,
            nullable=True,
            checks=Check.isin(["Yes", "No"]),
        ),
    },
    coerce=True,
    strict=False,
    name="EncounterSchema",
)


LabTestSchema = DataFrameSchema(
    columns={
        "lab_id": Column(str, nullable=False),
        "encounter_id": Column(str, nullable=False),
        "test_name": Column(str, nullable=True),
        "test_date": Column(str, nullable=True),
        "status": Column(
            str,
            nullable=True,
            checks=Check.isin(["Preliminary", "Final", "Corrected", "Cancelled"]),
        ),
    },
    coerce=True,
    strict=False,
    name="LabTestSchema",
)


ClaimsSchema = DataFrameSchema(
    columns={
        "billing_id": Column(str, nullable=False),
        "patient_id": Column(str, nullable=False),
        "encounter_id": Column(str, nullable=False),
        "billed_amount": Column(
            float,
            nullable=True,
            checks=Check.greater_than_or_equal_to(0),
        ),
        "paid_amount": Column(
            float,
            nullable=True,
            checks=Check.greater_than_or_equal_to(0),
        ),
        "claim_status": Column(
            str,
            nullable=True,
            checks=Check.isin(["Paid", "Denied", "Pending", "Adjusted"]),
        ),
    },
    coerce=True,
    strict=False,
    name="ClaimsSchema",
)


# ---------------------------------------------------------------------------
# Validation result container
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Encapsulates the outcome of a single schema validation run."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    row_count: int = 0
    valid_row_count: int = 0
    schema_name: str = ""

    @property
    def invalid_row_count(self) -> int:
        return self.row_count - self.valid_row_count

    @property
    def error_rate(self) -> float:
        if self.row_count == 0:
            return 0.0
        return round(self.invalid_row_count / self.row_count, 4)

    def summary(self) -> dict:
        return {
            "schema_name": self.schema_name,
            "is_valid": self.is_valid,
            "row_count": self.row_count,
            "valid_row_count": self.valid_row_count,
            "invalid_row_count": self.invalid_row_count,
            "error_rate": self.error_rate,
            "error_count": len(self.errors),
            "errors": self.errors[:20],  # cap for readability
        }


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------

def validate_dataframe(
    df: pd.DataFrame,
    schema: DataFrameSchema,
    schema_name: str,
) -> ValidationResult:
    """
    Validate *df* against *schema* and return a :class:`ValidationResult`.

    Uses pandera's ``lazy=True`` mode so that ALL column errors are collected
    before raising rather than stopping at the first failure.
    """
    row_count = len(df)
    errors: List[str] = []

    logger.debug(f"Validating {row_count:,} rows against {schema_name} …")

    try:
        schema.validate(df, lazy=True)
        logger.info(f"{schema_name}: all {row_count:,} rows passed validation.")
        return ValidationResult(
            is_valid=True,
            errors=[],
            row_count=row_count,
            valid_row_count=row_count,
            schema_name=schema_name,
        )

    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases

        # Build human-readable error messages
        for _, row in failure_cases.iterrows():
            msg = (
                f"Column '{row.get('column')}' | "
                f"Check '{row.get('check')}' | "
                f"Failed value: {row.get('failure_case')}"
            )
            errors.append(msg)

        # Count distinct failing row indices
        failed_indices: set = set()
        if "index" in failure_cases.columns:
            failed_indices = set(failure_cases["index"].dropna().astype(int).tolist())

        valid_row_count = row_count - len(failed_indices)

        logger.warning(
            f"{schema_name}: {len(failed_indices):,} rows failed validation "
            f"({len(errors)} distinct failure cases)."
        )

        return ValidationResult(
            is_valid=False,
            errors=errors,
            row_count=row_count,
            valid_row_count=max(valid_row_count, 0),
            schema_name=schema_name,
        )

    except Exception as exc:  # noqa: BLE001
        error_msg = f"Unexpected validation error: {exc}"
        logger.error(error_msg)
        return ValidationResult(
            is_valid=False,
            errors=[error_msg],
            row_count=row_count,
            valid_row_count=0,
            schema_name=schema_name,
        )
