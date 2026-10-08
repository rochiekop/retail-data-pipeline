# 05: Bronze — Merchandising Catalog

**What to build:** The Merchandising Catalog workbook is loaded into Bronze unchanged, as text, tagged with a checksum of the file and the load time. If the same file (same checksum) has already been loaded, nothing is loaded again. An edited file is loaded as a new version. A missing file, or a workbook missing a sheet or a column, fails the run. Blank rows are ignored, and numbers typed into Excel are stored as text.

**Blocked by:** 04: Bronze — one Business Date from the Shop Database

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 5

- [ ] Both sheets load into Bronze with the file's checksum
- [ ] Loading an unchanged file a second time loads nothing
- [ ] An edited file loads as a new version
- [ ] A missing file, missing sheet or changed columns fail with a clear error
- [ ] Blank rows are skipped, and numeric cells are stored as text
- [ ] A row whose trailing cells are blank (Excel stores fewer cells) loads with those columns as NULL

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest tests/test_bronze_catalog.py -v` shows 8 passed
- Every earlier test still passes: `.venv/Scripts/python -m pytest -v`
