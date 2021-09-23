import os
import time
import urllib

URL = "https://de.wikipedia.org/wiki/Liste_der_Tatort-Folgen"


def read_web_page(filename: str) -> str:
    with open(filename, "r") as infile:
        return infile.read()


def download(output: str):
    if os.path.exists(output):
        now = time.time()
        os.rename(output, f"output.{now}.backup")

    page = urllib.URLOpener()
    page.retrieve(URL, output)

