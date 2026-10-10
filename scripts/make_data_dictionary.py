"""Write docs/data_dictionary.md from the gold table descriptions in src/gbdemo/catalog_docs.py.

Usage: python scripts/make_data_dictionary.py
"""

from pathlib import Path

from gbdemo.catalog_docs import markdown

OUT = Path(__file__).resolve().parents[1] / "docs" / "data_dictionary.md"

if __name__ == "__main__":
    OUT.write_text(markdown(), encoding="utf-8", newline="\n")
    print(f"wrote {OUT}")
