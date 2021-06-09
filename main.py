#!/usr/bin/env python3

import datetime
import io
import os
import sys
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from tqdm import tqdm

BASE_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
DOWNLOAD_DIRECTORY = BASE_DIR / "download"
RESULT_DIRECTORY = BASE_DIR / "result"

MASTER_TXT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
MASTER_TXT_DOWNLOAD_PATH = DOWNLOAD_DIRECTORY / "masterfilelist.txt"


def prepare_directories():
    DOWNLOAD_DIRECTORY.mkdir(parents=True, exist_ok=True)
    RESULT_DIRECTORY.mkdir(parents=True, exist_ok=True)


def download_master_txt(
    url: str = MASTER_TXT_URL, download_path: Path = MASTER_TXT_DOWNLOAD_PATH
):
    print("Download master txt file")
    expected_content_type = "text/plain"
    head = requests.head(url, allow_redirects=True)
    head.raise_for_status()
    if (content_type := head.headers.get("content-type")) != expected_content_type:
        raise RuntimeError("expected a text file but received", content_type)

    result = requests.get(url)
    result.raise_for_status()
    if (content_type := result.headers.get("content-type")) != expected_content_type:
        raise RuntimeError("expected a text file but received", content_type)

    with open(download_path, "wb") as f:
        print("Save master txt file")
        f.write(result.content)


def master_txt_to_dataframe(
    begin_date: pd.Timestamp,
    end_date: pd.Timestamp,
    file_path: Path = MASTER_TXT_DOWNLOAD_PATH,
) -> pd.DataFrame:
    print("Loading master txt into dataframe")
    df = pd.read_csv(file_path, sep=" ", header=None, names=["url"], usecols=[2])
    df = df[df["url"].str.contains("export.CSV", na=False)]
    df["datetime"] = pd.to_datetime(df["url"].str.extract(r"gdeltv2\/(\d+)")[0])
    df.set_index("datetime", drop=True, inplace=True)
    return df.loc[begin_date:end_date]


def download_extract_csv_zip(url: str, country_code: str = "KOR") -> pd.DataFrame:
    result = requests.get(url)
    result.raise_for_status()
    zipfile = ZipFile(io.BytesIO(result.content))
    output_dfs = []
    for name in zipfile.namelist():
        data = zipfile.read(name)
        df = pd.read_csv(io.BytesIO(data), sep="\t", header=None, index_col=0)
        df = df[
            (df[5] == country_code)
            | (df[7] == country_code)
            | (df[15] == country_code)
            | (df[17] == country_code)
            | (df[37] == country_code)
            | (df[53] == country_code)
        ]
        output_dfs.append(df)
    return pd.concat(output_dfs, axis=0, copy=False)


if __name__ == "__main__":
    prepare_directories()
    download_master_txt()
    master_df = master_txt_to_dataframe("2016-01-01", "2016-12-31")
    start_time = datetime.datetime.now()
    print(f"Begin downloading {len(master_df['url'])} urls")
    print(f"{start_time}")
    csv_dfs = []
    for i, url in enumerate(tqdm(master_df["url"])):
        try:
            csv_dfs.append(download_extract_csv_zip(url))
        except:
            print(f"Skipping i={i} url={url} due to error", sys.exc_info()[0])

    csv_df = pd.concat(csv_dfs, axis=0, copy=False)
    csv_df.to_csv(RESULT_DIRECTORY / "2016_korea.csv", header=False)
