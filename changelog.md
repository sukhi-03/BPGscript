## 01-07-25
- Payer Hierarchy excel input updated as of Datamodel v0.4
- Fixed checkpoint logic, now we can technically pause and resume the script
- 'Beneficiary type' field is now 'Channel' and 'Sub-channel' according to new data model
- Employed a rough fuzzy map logic for channel names

## 22-07-25
- Aditya streamlined/automated some % of the BPG_search_pdf logic
- multi-value_fix.py was earlier being used to split and explode the cells in BPG where one cell had multiple values; Now its converted to post_gemini-camelot.py as a standard postprocessing script to be ran after gemini_camelot.py