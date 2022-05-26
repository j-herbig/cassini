import os

URL_ROOT = "https://transtats.bts.gov/"
URL_ZIP_PREFIX = "PREZIP/"
URL_WEBSITE = "DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
DOWNLOAD_PATH = "data/"
DOWNLOAD_PATH_LOOKUP = os.path.join(DOWNLOAD_PATH, "lookup_tables")
YEARS = 2019
ZIP_PREFIX = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
CSV_PREFIX = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_"
