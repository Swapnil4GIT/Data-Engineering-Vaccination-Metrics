import pytest
import os
import sys
from typing import Callable, NoReturn
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.housekeeping import housekeeping
from Application.dataarchiveandpurge import archive_and_purge

housekeep = housekeeping()
logger = housekeep.test_log()
darch = archive_and_purge()

def test_dataarchiveandpurge_rc_1():
    return_code = darch.job_archive_datasets(logger)
    assert return_code in [0]

