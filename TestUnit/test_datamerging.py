import pytest
import os
import sys
from typing import Callable, NoReturn
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.housekeeping import housekeeping
from Application.datamerging import datamerging

housekeep = housekeeping()
logger = housekeep.test_log()
dmerge = datamerging()

def test_datamerging_rc_1():
    return_code = dmerge.job_Datamerging(logger)
    assert return_code in [0]

