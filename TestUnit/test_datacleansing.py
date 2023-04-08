import pytest
import os
import sys
import pandas as pd
from typing import Callable, NoReturn

sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.datacleansing import datacleansing
from Application.housekeeping import housekeeping
dataclean = datacleansing()
housekeep = housekeeping()

logger = housekeep.test_log()
filepath = housekeep.getSource_File_Path(logger)
files = os.listdir(filepath)
files = [f for f in files if not f.startswith('.')]

@pytest.fixture
def ind_dataset_columns() -> list[str]:
    """
    Get the columns of the IND.csv dataframe and validate the same.
    """
    fname = filepath + 'IND.csv'
    df = pd.read_csv(fname)
    df_ind_dataset_columns = df.columns.to_list()
    return df_ind_dataset_columns

def test_ind_dataset_columns(ind_dataset_columns: Callable) -> NoReturn:
    """
    Check the input dataset columns are as expected.
    """
    expected_columns = ['ID',
                        'Name',
                        'DOB',
                        'VaccinationType',
                        'VaccinationDate',
                        'Free or Paid']
    assert expected_columns == ind_dataset_columns

@pytest.fixture
def usa_dataset_columns() -> list[str]:
    """
    Get the columns of the IND.csv dataframe and validate the same.
    """
    fname = filepath + 'USA.csv'
    df = pd.read_csv(fname)
    df_usa_dataset_columns = df.columns.to_list()
    return df_usa_dataset_columns

def test_usa_dataset_columns(usa_dataset_columns: Callable) -> NoReturn:
    """
    Check the input dataset columns are as expected.
    """
    expected_columns = ['ID', 'Name', 'VaccinationType', 'VaccinationDate']
    assert expected_columns == usa_dataset_columns

@pytest.fixture
def aus_dataset_columns() -> list[str]:
    """
    Get the columns of the IND.csv dataframe and validate the same.
    """
    fname = filepath + 'AUS.xlsx'
    df = pd.read_excel(fname)
    df_aus_dataset_columns = df.columns.to_list()
    return df_aus_dataset_columns

def test_aus_dataset_columns(aus_dataset_columns: Callable) -> NoReturn:
    """
    Check the input dataset columns are as expected.
    """
    expected_columns = ['Unique ID',
                        'Patient Name',
                        'Vaccine Type',
                        'Date of Birth',
                        'Date of Vaccination']
    assert expected_columns == aus_dataset_columns


def test_datacleansing():
    return_code = dataclean.job_Datacleansing(logger)
    assert return_code == 0
