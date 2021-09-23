"""
Tatort utility script

zeigt Lücken, Episode und anderes Zeug an

Optionen
-k, --kurz              nur Episode Nummern und Titel anzeigen

Beispiele:
tatort.py info --gesendet --kurz        Kurze info auf alle gesendeten Shows (Quelle: Wikipedia)
tatort.py info --nochnicht              Lange liste der Dateien die ich nicht habe
tatort.py info --nochnicht --luecken    Versuche die Luecken anzuzeigen

tatort.py runterladen                   Download der Folgen von Wikipedia
tatort.py n5550-lesen                   Inventar der Dateien auf N5550
"""
from typing import Optional, List, Tuple

from collections import namedtuple
import argparse
import re
import datetime
from itertools import groupby, count

import pandas as pd
import bs4
import arrow

import data.tatort_data as tatort_data

import ingress.html.read
import ingress.smb.N5550

EPISODE_FILE = "data/Liste der Tatort-Folgen – Wikipedia.html"

REGEX_LOCAL_FILENAME = re.compile(
    r"^(?P<fileinfo>.*?)Tatort - (?P<folge>[\dabcde]+) - (?P<st>\d+-\d+-\d+) - (?P<ermittler>[\w,\.\s]+)(\s*(?P<fall>\([,\-\d]+\)))? - (?P<titel>.*?)\.(?P<fileformat>\w+)"
)

REGEX_GERMAN_DATE_REGEX = re.compile(
    r"(?P<tag>\d{1,2})\.\s(?P<monat>Jan|Feb|Mar|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dec)\.\s(?P<jahr>\d{4})"
)

TatortData = namedtuple("TatortData", "all have")


def make_filename(
    folge: int, sendetag: str, ermittler: str, fall: Optional[str], titel: str
) -> str:
    if fall:
        return f"Tatort - {folge:03n} - {sendetag} - {ermittler} ({fall}) - {titel}"
    return f"Tatort - {folge} - {sendetag} - {ermittler} - {titel}"


def get_all_episodes() -> pd.DataFrame:
    list_content = ingress.html.read.read_web_page(EPISODE_FILE)
    soup = bs4.BeautifulSoup(list_content, "html.parser")
    table = soup.find(class_="wikitable sortable jquery-tablesorter")
    rows = table.find_all("tr")
    data = []
    count = 0
    # parsing of table
    for content_row in rows[1:]:
        cells = content_row.find_all("td")
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
                    tag = int(parts.get("tag", 1))
                    monat = parts.get("monat", "Jan")
                    jahr = int(parts.get("jahr", 1970))
                    monat_lookup = {
                        "jan": 0,
                        "feb": 1,
                        "mar": 2,
                        "apr": 3,
                        "mai": 4,
                        "jun": 5,
                        "jul": 6,
                        "aug": 7,
                        "sep": 8,
                        "okt": 9,
                        "nov": 10,
                        "dez": 11,
                    }
                    monat_no = monat_lookup.get(monat.lower()) + 1
                    st = arrow.get(datetime.datetime(jahr, monat_no, tag))
                    break

        sendetag = st.format("YYYY-MM-DD")
        # ermittler
        ermittler = str(cells[4].contents[0].contents[0])
        ermittler = ermittler.replace(" und", ",")
        # fall
        fall = cells[5].contents[0].strip()
        if " " in fall:
            fall = int(fall.split(" ")[0])
        else:
            fall = int(fall)
        # filename
        file = make_filename(folge, sendetag, ermittler, fall, titel)

        data.append(
            {
                "folge": folge,
                "titel": titel,
                "sender": sender,
                "sendetag": sendetag,
                "ermittler": ermittler,
                "fall": fall,
                "filename": file,
            }
        )
    data = sorted(data, key=lambda x: x["folge"])
    df_all: pd.DataFrame = tatort_data.make_df(data)
    df_all.set_index(keys="folge", inplace=True)
    return df_all


def get_existing_episodes() -> pd.DataFrame:
    existing_files = ingress.smb.N5550.read_file_list("ingress/smb/N5550-file-list.txt")
    existing_data = []
    for file in existing_files:
        # skip supplemental files
        if (
            file.endswith(".srt")
            or file.endswith(".ttml")
            or file.endswith("./")
            or file.endswith("../")
        ):
            continue
        # Tatort - 001 - 1970-11-29 - Trimmel - Taxi nach Leipzig.avi
        match = REGEX_LOCAL_FILENAME.match(file)
        if match:
            parts = match.groupdict()
            # folge
            folge = parts.get("folge")
            if folge.endswith("a"):
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
            filename = file.replace(".avi", "").replace(".mkv", "").replace(".mp4", "")
            existing_data.append(
                {
                    "folge": folge,
                    "titel": titel,
                    "sender": sender,
                    "sendetag": sendetag,
                    "ermittler": ermittler,
                    "fall": fall,
                    "filename": filename,
                }
            )
        else:
            print(f"Could not match: {file}\n")
    existing_data = sorted(existing_data, key=lambda x: x["folge"])
    df_have: pd.DataFrame = tatort_data.make_df(existing_data)
    df_have.set_index(keys="folge", inplace=True)
    return df_have


def compare(df_all: pd.DataFrame, df_have: pd.DataFrame) -> List[int]:
    missing = []
    for folge in df_all.index.to_list():
        if folge not in df_have.index:
            missing.append(folge)

    return missing


def analyze(df_all: pd.DataFrame, df_have: pd.DataFrame) -> List[int]:
    percent = float(len(df_have)) / float(len(df_all))
    print(
        f"Comparing all {len(df_all)} episodes to existing ({len(df_have)}) episodes - {percent:%} complete"
    )
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
                print(
                    f"Filename mismatch\n"
                    f"Expected: {should.filename}\n"
                    f"Got:      {have.filename}\n"
                )
        else:
            missing.append(folge)

    return missing


def show_existing(data: TatortData, kurz: Optional[bool] = False):
    pass


def show_have(data: TatortData, kurz: Optional[bool] = False):
    pass


def show_missing(
    data: TatortData, kurz: Optional[bool] = False, gaps: Optional[bool] = False
):
    missing = compare(data.all, data.have)

    if gaps:
        print("Fehlende Episoden in Gruppen")
        missing_groups = groupby(missing, lambda n, c=count(): n - next(c))
        for gap in missing_groups:
            elements = list(gap[1])
            if kurz:
                if len(elements) > 1:
                    print(f"{elements[0]+1:4} - {elements[-1]+1:4},", end="")
                else:
                    print(f"{elements[0]+1:4},", end="")

        print()
        return

    print("Fehlende Episoden:")
    for episode in missing:
        entry = data.all.iloc[episode]
        if kurz:
            print(f"{episode+1:4}")
        else:
            print(entry.filename)


def find(data: TatortData, kurz: Optional[bool] = False):
    pass


def load_data() -> TatortData:
    df_all = get_all_episodes()
    df_have = get_existing_episodes()
    return TatortData(df_all, df_have)


def download():
    ingress.html.read.download(EPISODE_FILE)


def inventory():
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument(
        "--kurz", help="Kurzer output", action="store_true", default=False
    )

    subparser = parser.add_subparsers()
    parser.set_defaults(show=True)
    show_parser = subparser.add_parser("info")
    show_parser.add_argument(
        "what_to_show",
        nargs="?",
        default="nochnicht",
        choices=["nochnicht", "gesendet", "habe"],
    )
    show_parser.add_argument(
        "--luecken",
        default=False,
        action="store_true",
        help="zeige Luecken an, nicht einzelne Werte",
    )

    find_parser = subparser.add_parser("finde")
    parser.set_defaults(find=False)
    find_parser.add_argument(
        "what_to_find",
        nargs="?",
        default="episode",
        choices=["episode", "title", "datum"],
    )

    parser.set_defaults(download=False)
    _ = subparser.add_parser("runterladen")

    parser.set_defaults(inventory=False)
    _ = subparser.add_parser("n5550-lesen")

    parser.set_defaults(analyse=False)
    _ = subparser.add_parser("analyse")

    return parser.parse_args()


def main():
    args = parse_args()

    if args.download:
        download()
        print("Success.")
        return

    the_data = load_data()

    if args.show:
        if args.what_to_show == "habe":
            show_have(the_data, args.kurz)
        elif args.what_to_show == "gesendet":
            show_existing(the_data, args.kurz)
        elif args.what_to_show == "nochnicht":
            show_missing(the_data, args.kurz, args.luecken)
        return

    if args.find:
        find(the_data, args.kurz)
        return

    if args.analyze:
        analyze(the_data.all, the_data.have, args)
        return


if __name__ == "__main__":
    main()
