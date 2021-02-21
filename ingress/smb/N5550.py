from typing import List


def read_file_list(filename: str) -> List[str]:
    with open(filename, "r") as infile:
        content = infile.read()

    return content.split("\n")

