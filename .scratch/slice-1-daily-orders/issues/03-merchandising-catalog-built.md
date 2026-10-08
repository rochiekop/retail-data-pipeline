# 03: Merchandising Catalog built

**What to build:** One command generates the Merchandising Catalog, an Excel workbook built from the Olist product and category-translation files. This simulates the files the merchandising team maintains. The workbook has a `products` sheet and a `category_translation` sheet, with values copied unchanged and empty cells left blank. The translation file starts with an invisible UTF-8 byte-order mark (BOM), which must not break the column check.

**Blocked by:** 02: Shop Database seeded from Olist

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 3. Review Focus item 2 applies.

- [ ] The workbook has the sheets `products` and `category_translation`, in that order
- [ ] Header rows match the source contract exactly, with no BOM characters
- [ ] A product with no category has an empty category cell
- [ ] The output folder is created if it doesn't exist
- [ ] Changed source columns fail with an error naming the file

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest tests/test_build_catalog.py -v` shows 5 passed
- Run `.venv/Scripts/python -m pipeline.seed catalog`, then open `data/catalog/merchandising_catalog.xlsx` in Excel and look at both sheets
