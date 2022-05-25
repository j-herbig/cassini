import os
import requests
import zipfile
from tqdm import tqdm
import download_params

import warnings
warnings.filterwarnings("ignore")


def get_download_params() -> tuple:
    """Gets parameters for downloads from download_params.py

    Returns:
        tuple: Parameters for downloading, saving and unzipping files.
    """
    return (
        download_params.URL_ROOT, 
        download_params.DOWNLOAD_PATH, 
        download_params.YEARS, 
        download_params.ZIP_PREFIX, 
        download_params.CSV_PREFIX
    )


def download_file(url_dl: str, path: str, file_name: str):
    """Downloads a file and writes it to disk.

    Args:
        url_dl (str): Download URL of the file. 
        path (str): Path to save the file in.
        file_name (str): Name of the file.
    """
    # check if file already exists
    if not os.path.exists(path):
        os.mkdir(path)

    response = requests.get(url_dl, timeout=50, verify=False)
    # write file to disk
    with open(os.path.join(path, file_name), 'wb') as f:
        f.write(response.content)


def unzip_file(path_zip: str, file_to_extract: str, path_target: str = None, delete_zip: bool = False):
    """Unzips a zip file.

    Args:
        path_zip (str): Path to the zip file.
        file_to_extract (str): Name of the zip file.
        path_target (str, optional): Path to save the unzipped file in. Defaults to None.
        delete_zip (bool, optional): Whether or not to delete the zip file after unzipping. Defaults to False.
    """
    if path_target != None: 
        if not os.path.exists(path_target):
            os.mkdir(path_target)
    else:
        path_target = "/".join(path_zip.split("/")[:-1])
    
    if not os.path.exists(os.path.join(path_target, file_to_extract)):
        try:
            with zipfile.ZipFile(path_zip) as z:
                with open(os.path.join(path_target, file_to_extract), 'wb') as f:
                    f.write(z.read(file_to_extract))
        except Exception as e:
            print(f"Error when unzipping {path_zip}: {e}")
    
    if delete_zip:
        os.remove(path_zip)


def download_and_unzip_years(url_root: str, path: str, years: int or list, zip_prefix: str, csv_prefix: str):
    """Downloads and unzips a file.

    Args:
        url_root (str): Root URL to the file to download.
        path (str): Path to save the downloaded file at.
        years (int or list): Year or list of years to download data for.
        zip_prefix (str): Prefix of the zip files.
        csv_prefix (str): Prefix of the csv files.
    """
    if type(years) == int:
        years = [years]

    with tqdm(total=len(years) * 12) as pbar:
        for year in years:
            for month in range(1, 13):
                url_download = url_root + zip_prefix + f"{year}_{month}.zip"
                name_zip = zip_prefix + f"{year}_{month}.zip"
                name_csv = csv_prefix + f"{year}_{month}.csv"
                path_zip = os.path.join(path, name_zip)
                
                if not os.path.isfile(path_zip):
                    download_file(url_download, path, name_zip)
                unzip_file(path_zip, name_csv, path_target=None, delete_zip=True)
                # print()
                pbar.update(1)


if __name__ == "__main__":
    url_root, path, years, zip_prefix, csv_prefix = get_download_params()
    download_and_unzip_years(url_root, path, years, zip_prefix, csv_prefix)