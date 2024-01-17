# mm_phys_audit
these scripts are for performing the biannual medical marijuana physician audit  
`mm1.py` should be run first and generates 2 files:
  - `mm_manual.csv` - a list of physicians who could not be matched to a physician in awarxe, they must be manually reviewed
  - `mm_matches_combined.csv` - a list of physicians who were successfully matched to a physician in awarxe  

`mm2.py` should be run after updating `mm_manual.csv` and generates the final report: `mmq.xlsx`