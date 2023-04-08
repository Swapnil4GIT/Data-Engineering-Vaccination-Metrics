import pytest
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.datacleansing import datacleansing
from Application.housekeeping import housekeeping
dataclean = datacleansing()
housekeep = housekeeping()

def test_datacleansing():
    logger = housekeep.test_log()
    return_code = dataclean.job_Datacleansing(logger)
    assert return_code == 0
