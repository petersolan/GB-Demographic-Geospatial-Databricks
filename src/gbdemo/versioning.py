"""Versioned lookups and boundaries (slowly changing dimension, type 2).

Job ad: "Version lookups and boundaries so historical outputs stay reproducible".
Each lookup row carries valid_from / valid_to and the source release it came from,
so any output can be rebuilt exactly as it was for a given release.
"""
