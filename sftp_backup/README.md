# sftp_backup  
these scripts backup the vendor and pmp sftps to the google drive daily  
`vendor_sftp_backup.py` and `pmp_sftp_backup.py` are for running on a local machine as needed  
`cloud_functions_sftp_backup.py` is a cloud function and scheduled to run `30 8 * * *` and has two versions in the cloud project:
- `sftp_backup` a version for the vendor sftp
- `pmp_sftp_backup` a version for the pmp sftp