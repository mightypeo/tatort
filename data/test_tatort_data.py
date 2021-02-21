import unittest

import numpy as np
import pandas as pd

import data.tatort_data as TN

class TestTatortData(unittest.TestCase):
    def test_make_df(self):
        actual = TN.make_df()
        self.assertIsInstance(actual, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
