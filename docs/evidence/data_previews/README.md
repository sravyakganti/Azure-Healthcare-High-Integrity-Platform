# Data Preview — Bronze vs Silver PII Masking Proof

These two files contain **identical 5 patients** at different pipeline stages.
They demonstrate that SHA-256 masking is applied correctly before any data
reaches the Silver layer or Azure Synapse.

## What changed between Bronze and Silver?

| Column | Bronze (raw) | Silver (masked) |
|--------|-------------|-----------------|
| `first_name` | `Danielle` | **DROPPED** |
| `last_name` | `Johnson` | **DROPPED** |
| `address` | `32181 Johnson Course Apt. 389` | **DROPPED** |
| `email` | `danielle.johnson40@example.com` | **DROPPED** |
| `phone` | `(615)759 407` | **DROPPED** |
| `registration_date` | `03-01-2025` | **DROPPED** |
| `dob` | `14-05-1940` (string) | `1940-05-14` (ISO date32) |
| `first_name_hashed` | *(absent)* | `c1144dfc…d49451bd` (SHA-256) |
| `last_name_hashed` | *(absent)* | `3013b18f…16beda66` (SHA-256) |
| `email_hashed` | *(absent)* | `298f6b76…985ab25` (SHA-256) |
| `address_hashed` | *(absent)* | `199c0b5b…3c8ca46` (SHA-256) |

## How to verify a hash

```python
import hashlib
name = "Danielle"
print(hashlib.sha256(name.strip().encode("utf-8")).hexdigest())
# c1144dfca7ce1f5d8311ba7566a3cbcc7f553025a03e5df6a61b0f17d49451bd
```

The hash is **deterministic** — the same input always produces the same
64-character hex digest — enabling cross-dataset record linkage without
exposing the raw value.
