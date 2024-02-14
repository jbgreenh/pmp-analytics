# sftp_backup  
these scripts backup the vendor and pmp sftps to the google drive daily    
`sftp_backup.py` is for running on a local machine as needed and takes a command line argument:  
- `vendor` for backing up the vendor sftp  
- `pmp` for backing up the pmp sftp 

`cloud_functions_sftp_backup.py` is a cloud function and scheduled to run `30 8 * * *` and has two versions in the cloud project:  
- `sftp_backup` a version for the vendor sftp
- `pmp_sftp_backup` a version for the pmp sftp