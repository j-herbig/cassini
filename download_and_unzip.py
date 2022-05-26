import os
import requests
import zipfile
from tqdm import tqdm
import download_params
from bs4 import BeautifulSoup as bs

import warnings
warnings.filterwarnings("ignore")


def get_download_params() -> tuple:
    """Gets parameters for downloads from download_params.py

    Returns:
        tuple: Parameters for downloading, saving and unzipping files.
    """
    return (
        download_params.URL_ROOT,
        download_params.URL_ZIP_PREFIX,
        download_params.URL_WEBSITE,
        download_params.DOWNLOAD_PATH,
        download_params.DOWNLOAD_PATH_LOOKUP ,
        download_params.YEARS,
        download_params.ZIP_PREFIX,
        download_params.CSV_PREFIX,
    )


def download_file(url_dl: str, path: str, file_name: str = None):
    """Downloads a file and writes it to disk.

    Args:
        url_dl (str): Download URL of the file. 
        path (str): Path to save the file in.
        file_name (str): Name of the file, if None, file name is read from header of response. Defaults to None.
    """
    
    # if path doesn't exist, make it
    if not os.path.exists(path):
        os.mkdir(path)

    # request download url
    response = requests.get(url_dl, timeout=50, verify=False)

    # if the file name was not specified, get it from download server
    if not file_name:
        try:
            file_name = response.headers["Content-Disposition"][
                response.headers["Content-Disposition"].find("filename=") + len("filename="):
            ]
        except Exception as e:
            print(f"file name could not be read from server for {url_dl}")
            print("download gets skipped")

    # write file to disk
    if file_name:
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
    # if target path was specified, make it, if it doesn't exist
    # else use origin path of zip as target path
    if path_target != None: 
        if not os.path.exists(path_target):
            os.mkdir(path_target)
    else:
        path_target = "/".join(path_zip.split("/")[:-1])
    
    # if the file to extract doesn't exist, extract from zip file
    if not os.path.exists(os.path.join(path_target, file_to_extract)):
        try:
            with zipfile.ZipFile(path_zip) as z:
                with open(os.path.join(path_target, file_to_extract), 'wb') as f:
                    f.write(z.read(file_to_extract))
        except Exception as e:
            print(f"Error when unzipping {path_zip}: {e}")
    
    # delete zip file if necessary
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
    # if years was passed as integer, make it a list
    if type(years) == int:
        years = [years]

    print("Processing data files...")

    # use a progress bar to visualize file processing
    with tqdm(total=len(years) * 12) as pbar:
        # download process files for all months in the specified years
        for year in years:
            for month in range(1, 13):
                # make url, names and path for downloading and naming files
                url_download = url_root + zip_prefix + f"{year}_{month}.zip"
                name_zip = zip_prefix + f"{year}_{month}.zip"
                name_csv = csv_prefix + f"{year}_{month}.csv"
                path_zip = os.path.join(path, name_zip)

                # download and unzip the file if csv, respectively zip file doesn't exist
                if not os.path.isfile(os.path.join(path, name_csv)):
                    if not os.path.isfile(path_zip):
                        download_file(url_download, path, name_zip)
                    unzip_file(path_zip, name_csv, path_target=None, delete_zip=True)
                
                # proceed progress bar
                pbar.update(1)


def get_lookup_urls(url_root: str, download_root: str) -> list:
    """Gets download URLs for llokup tables.

    Args:
        url_root (str): URL to website to search for look up table download links.
        download_root (str): Root URL for downloading for downloading look up tables

    Returns:
        list: Download URLs for look up tables.
    """
    # request url and parse it to make download links findable
    r = requests.get(url_root, timeout=10, verify=False)
    soup = bs(r.text, features="html.parser")
    download_urls = []

    # find all download links for look up tables and add it to the list
    for i, link in enumerate(soup.findAll('a')):
        try:
            if soup.select('a')[i].attrs['title'] == "Download Lookup data":
                download_urls.append(download_root + link.get('href'))
        except KeyError:
            continue
    return download_urls


def download_lookup_tables(url_root: str, download_root: str, path: str):
    """Downloads look up tables as csv files.

    Args:
        url_root (str): URL to website to search for look up table download links.
        download_root (str): Root URL for downloading for downloading look up tables.
        path (str): Path to save look up table csv files.
    """
    print("Processing look up tables...")

    # get download links for look up tables and download them
    for url_dl in tqdm(get_lookup_urls(url_root, download_root)):
        download_file(url_dl, path)


if __name__ == "__main__":
    # get urls, paths and other parameters necessary to process files
    url_root, url_zip_prefix, url_website, download_path, download_path_lookup, years, zip_prefix, csv_prefix = get_download_params()

    # download and unzip data of whole years
    download_and_unzip_years(url_root + url_zip_prefix, download_path, years, zip_prefix, csv_prefix)

    # download look up tables
    download_lookup_tables(url_root + url_website, url_root, download_path_lookup)
    