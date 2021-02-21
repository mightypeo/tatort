from typing import List, Optional

import numpy as np
import pandas as pd


def make_df(data: Optional[List] = []) -> pd.DataFrame:
    return pd.DataFrame(
        data, columns=["folge", "titel", "sender", "sendetag", "ermittler", "fall", "filename"]
    )
