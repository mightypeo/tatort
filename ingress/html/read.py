import numpy as np
import pandas as pd

from bs4 import BeautifulSoup

def read_web_page( filename: str) -> str:
    with open(filename, "r") as infile:
        return infile.read()

