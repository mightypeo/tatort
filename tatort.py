from typing import Optional, List
import argparse
import re
import datetime

import numpy as np
import pandas as pd
import bs4
import arrow

import data.tatort_data as tatort_data

import ingress.html.read
import ingress.smb.N5550

REGEX_LOCAL_FILENAME = re.compile(
    r"Tatort - (?P<folge>[\dabcde]+) - (?P<st>\d+-\d+-\d+) - (?P<ermittler>[\w,\.\s]+)(\s*(?P<fall>\([,\-\d]+\)))? - (?P<titel>.*?)\.(?P<fileformat>\w+)")

REGEX_GERMAN_DATE_REGEX = re.compile(
    r'(?P<tag>\d{1,2})\.\s(?P<monat>Jan|Feb|Mar|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dec)\.\s(?P<jahr>\d{4})')


def make_filename(folge: int, sendetag: str, ermittler: str, fall: Optional[str], titel: str) -> str:
    if fall:
        return f"Tatort - {folge:03n} - {sendetag} - {ermittler} ({fall}) - {titel}"
    return f"Tatort - {folge} - {sendetag} - {ermittler} - {titel}"


def get_all_episodes() -> pd.DataFrame:
    input_file = "data/Liste der Tatort-Folgen – Wikipedia.html"
    list_content = ingress.html.read.read_web_page(input_file)
    soup = bs4.BeautifulSoup(list_content, 'html.parser')
    table = soup.find(class_='wikitable sortable jquery-tablesorter')
    rows = table.find_all('tr')
    data = []
    count = 0
    # parsing of table
    for content_row in rows[1:]:
        cells = content_row.find_all('td')
        # folge
        folge = int(cells[0].contents[0].strip())
        # titel
        titel = str(cells[1].contents[0].contents[0])
        # sender
        if isinstance(cells[2].contents[0], bs4.element.Tag):
            sender = cells[2].contents[0].contents[0].strip()
        else:
            sender = cells[2].contents[0].strip()
        # sendetag
        for item in cells[3].contents:
            if isinstance(item, bs4.NavigableString):
                match = REGEX_GERMAN_DATE_REGEX.match(item)
                if match:
                    parts = match.groupdict()
                    tag = int(parts.get('tag', 1))
                    monat = parts.get('monat', 'Jan')
                    jahr = int(parts.get('jahr', 1970))
                    monat_lookup = {"jan": 0, "feb": 1, "mar": 2,
                                    "apr": 3, "mai": 4, "jun": 5,
                                    "jul": 6, "aug": 7, "sep": 8,
                                    "okt": 9, "nov": 10, "dez": 11}
                    monat_no = monat_lookup.get(monat.lower()) + 1
                    st = arrow.get(datetime.datetime(jahr, monat_no, tag))
                    break

        sendetag = st.format("YYYY-MM-DD")
        # ermittler
        ermittler = str(cells[4].contents[0].contents[0])
        ermittler = ermittler.replace(' und', ',')
        # fall
        fall = cells[5].contents[0].strip()
        if ' ' in fall:
            fall = int(fall.split(' ')[0])
        else:
            fall = int(fall)
        # filename
        file = make_filename(folge, sendetag, ermittler, fall, titel)

        data.append({"folge": folge, "titel": titel, "sender": sender, "sendetag": sendetag, "ermittler": ermittler,
                     "fall": fall, "filename": file})
    data = sorted(data, key=lambda x: x["folge"])
    df_all: pd.DataFrame = tatort_data.make_df(data)
    df_all.set_index(keys="folge", inplace=True)
    return df_all


def get_existing_episodes() -> pd.DataFrame:
    existing_files = ingress.smb.N5550.read_file_list("ingress/smb/N5550-file-list.txt")
    existing_data = []
    for file in existing_files:
        # skip supplemental files
        if file.endswith(".srt") or file.endswith(".ttml"):
            continue
        # Tatort - 001 - 1970-11-29 - Trimmel - Taxi nach Leipzig.avi
        match = REGEX_LOCAL_FILENAME.match(file)
        if match:
            parts = match.groupdict()
            # folge
            folge = parts.get("folge")
            if folge.endswith('a'):
                folge = folge[:-1]
            folge = int(folge)
            # titel
            titel = parts.get("titel")
            # sender
            sender = ""
            # sendetag
            sendetag = parts.get("st")
            # ermittler
            ermittler = parts.get("ermittler")
            # fall
            fall = parts.get("fall")
            if fall and fall.startswith("(") and fall.endswith(")"):
                fall = fall[1:-1]
            # filename - remove extensions
            filename = file.replace('.avi', '').replace('.mkv', '').replace('.mp4', '')
            existing_data.append(
                {"folge": folge, "titel": titel, "sender": sender, "sendetag": sendetag, "ermittler": ermittler,
                 "fall": fall, "filename": filename})
        else:
            print(f"Could not match: {file}\n")
    existing_data = sorted(existing_data, key=lambda x: x["folge"])
    df_have: pd.DataFrame = tatort_data.make_df(existing_data)
    df_have.set_index( keys="folge", inplace=True)
    return df_have


def compare(df_all: pd.DataFrame, df_have: pd.DataFrame) -> List[int]:
    percent = float(len(df_have)) / float(len(df_all))
    print(f"Comparing all {len(df_all)} episodes to existing ({len(df_have)}) episodes - {percent:%} complete")
    missing = []
    folge_counts = df_have.index.value_counts().to_dict()
    for folge in df_all.index.to_list():
        should = df_all.loc[folge]
        if folge in df_have.index:
            if folge_counts[folge] > 1:
                print(f"{folge_counts[folge]} episodes for {folge}")
                continue

            have = df_have.loc[folge]
            if should.filename != have.filename:
                print(f"Filename mismatch\n"
                      f"Expected: {should.filename}\n"
                      f"Got:      {have.filename}\n")
        else:
            missing.append(folge)

    return missing


def main():
    df_all = get_all_episodes()
    df_have = get_existing_episodes()
    missing = compare(df_all, df_have)
    print("Missing episodes:")
    for episode in missing:
        entry = df_all.iloc[episode]
        print(entry.filename)


if __name__ == "__main__":
    main()
