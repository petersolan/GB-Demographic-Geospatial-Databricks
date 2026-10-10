"""Postcode standardisation and validation.

Job ad: "Standardise postcodes and enrich records using NHS and ONS postcode products".
Cleans free-text postcodes (case, spacing, stray characters), validates the UK format,
and prepares a join key for the NHS Postcode Directory (NHSPD), including terminated
postcodes. Unfixable values are routed to a quarantine table with a reason code.
"""
