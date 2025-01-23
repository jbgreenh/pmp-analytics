# pmp-analytics

updated version of [az-pmp-analytics](https://github.com/jbgreenh/AZ-PMP-analytics) using google drive api and polars

## setup

this project uses [uv](https://github.com/astral-sh/uv?tab=readme-ov-file)  
after installing `uv` on your system using the link above, use `uv sync` to install all dependencies

scripts can then be run using `uv run {script_name}.py`  
make sure the `required files` for the script to be run are in the `data` folder

## 3x3 Threshold Report

this script takes the number of patients provided by bamboo, updates the file to the google drive, and updates the 3x3 Threshold sheet in the google drive

### required files

| file         | description                                                  |
| ------------ | ------------------------------------------------------------ |
| `AZ 3x3.csv` | the AZ 3x3 recipient list csv emailed to us montly by bamboo |

## dhs_upload

takes the latest standard extract from the google drive and uploads it to the dhs sftp, after the upload, it also deletes the oldest file in the sftp folder for maintenance

## error_pharmacies

this script finds the pharmacy with the most errors, uploads the file to the google drive, and updates the pharmacy error sheet in the google drive

### required files

| file               | description                                                   |
| ------------------ | ------------------------------------------------------------- |
| `List Request.csv` | iGov>Reports>Snapshot Reports>List Request>Generator>Download |

## exclude_ndcs

this script updates `data/excluded_ndcs.csv` and prints the new opiate antagonist ndcs to exclude in AWARxE.

## mm_phys_audit

these scripts are for performing the biannual medical marijuana physician audit  
`mm1.py` should be run first and generates 2 files:

- `data/mm_manual.csv` - a list of physicians who could not be matched to a physician in awarxe, they must be manually reviewed
- `data/mm_matches_combined.csv` - a list of physicians who were successfully matched to a physician in awarxe

`mm2.py` should be run after updating `data/mm_manual.csv` and generates the final report: `data/mmq.xlsx`

### required files

files for `mm1.py`:
| file | description |
|---|---|
`mm_audit.csv` | the input file from ADHS |
`old_mm.xlsx` | the results of the previous audit |

---

files for `mm2.py`:
| file | description |
|---|---|
`mm_manual.csv` | generated by `mm1.py` |
`mm_matches_combined.csv` | generated by `mm1.py` |

## mu_extras

this script pulls the not in violation list for mandatory use and exludes prescribers with an `exclude until` date in the future from mandatory use results, it then adds information on repeat appearences on the manadatory use list

### required files

a mandatory use file: `{MONTH_NAME}{YEAR}_mandatory_use_full.csv`  
the date at the beginning should be entered as an argument when running the script:
`data/cs_active.txt` should also be updated before running

```
python mu_extras.py january2024
```

## naloxone

this script sends the weekly naloxone report to ADHS in an email  
it also saves the weekly file at `data/naloxone_{today}.xlsx`.

## pharmacy_cleanup

this script performs the weekly pharmacy cleanup and provides a link to the file on google drive for changing closed pharmacies in awarxe to exempt under manage pharmacies

### required files

| file                             | description                                                   |
| -------------------------------- | ------------------------------------------------------------- |
| `DelinquentDispenserRequest.csv` | AWARxE>Admin>Delinquent Pharmacies>Dowload CSV                |
| `List Request.csv`               | iGov>Reports>Snapshot Reports>List Request>Generator>Download |
| `pharmacies.csv`                 | AWARxE>Admin>Manage Pharmacies>Download CSV                   |

## pharmacy deas not in manage pharmacies

this script checks for pharmacy dea numbers that are active with the dea but not in manage pharmacies in awarxe and writes the results to `data/pharmacy_deas_not_in_mp.csv`

### required files

updated `data/cs_active.txt`  
`List Request.csv` list request in igov
`pharmacies.csv` compliance>manage pharmacies>download

## scorecard

this script updates the scorecard tracking sheet on google drive with prescriber search rates for opioid and benzodiazepine prescriptions  
a counterpart for this script runs `0 10 12 * *` on google cloud  
`scorecard.py` is for running on a local machine as needed

## sftp_backup

these scripts backup the vendor and pmp sftps to the google drive daily  
counterparts for this script run `30 8 * * *` on google cloud for both `vendor` and `pmp`  
`sftp_backup.py` is for running on a local machine as needed and takes a command line argument:

- `vendor` for backing up the vendor sftp
- `pmp` for backing up the pmp sftp

## unregistered_pharmacists

this script checks the monthly inspection list for pharmacist registration, adds information from igov, and then adds the results to the unregistered pharmacist report on google drive

### required files

| file               | description                                                   |
| ------------------ | ------------------------------------------------------------- |
| `List Request.csv` | iGov>Reports>Snapshot Reports>List Request>Generator>Download |
| `pharmacies.csv`   | AWARxE>Admin>Manage Pharmacies>Download CSV                   |

## unregistered_prescribers

this script checks the dea list for prescriber registration and emails unregistered prescriber information to their respective boards

### required files

updated `data/cs_active.txt`
