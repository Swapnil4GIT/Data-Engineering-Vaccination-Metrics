import pytest
import os
import sys
from typing import Callable, NoReturn
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.housekeeping import housekeeping
housekeep = housekeeping()
logger = housekeep.test_log()

@pytest.fixture
def get_files() -> list[str]:
    """
    Get number of files available at source location.
    """
    files = os.listdir('../Data')
    files = [f for f in files if not f.startswith('.')]
    return files 

def test_get_files(get_files: Callable) -> NoReturn:
    """
    Validate the input source datasets with the expected datasets.
    """
    expected = ['IND.csv', 'USA.csv', 'AUS.xlsx']
    assert sorted(expected) == sorted(get_files) 

def test_housekeeping_rc_1():
    return_code = housekeep.job_Housekeeping(logger)
    assert return_code in [0]

