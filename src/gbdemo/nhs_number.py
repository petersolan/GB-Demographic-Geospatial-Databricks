"""NHS number utilities: Modulus 11 check digit, synthetic numbers, keyed pseudonyms.

Job ad: "Apply access, pseudonymisation and disclosure controls to sensitive data".
Synthetic numbers use the 999 test range so they can never collide with real patients.
Pseudonyms are HMAC-SHA256 with a key held in a Databricks secret scope, never in code.
"""
