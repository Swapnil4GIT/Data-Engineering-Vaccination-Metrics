import pytest
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.housekeeping import housekeeping
housekeep = housekeeping()

def test_housekeeping():
    logger = housekeep.test_log()
    return_code = housekeep.job_Housekeeping(logger)
    assert return_code == 0
