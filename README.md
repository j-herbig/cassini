# Introduction

In this project, flight data of 2019 are analyzed. The final investigation is limited to flights with depature in San Francisco and focus on American Airlines. The used data can be found on the website of the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr).

# Requirements

- pyenv with Python: 3.9.4

## Setup

Use the requirements file in this repo to create a new environment.

```BASH
make setup

#or

pyenv local 3.9.4
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

# Download and unzip data files

After setting up the environment, download the data with this command:

```Bash
python download_and_unzip.py
```

The download script uses parameters of download_params.py, e.g. file paths. To change the download directory, it's recommended to do that by changing download_params.py.

# Create a database and read in data from files

Make the database:

```Bash
python db_setup.py
```

The database setup script uses parameters of db_params.py. To change the name of the database, it's recommended to do that by changing db_params.py.

# Data analysis

Analysis of the data is performed in descriptive_statistics.ipynb, which in the end also exports a pdf report.
