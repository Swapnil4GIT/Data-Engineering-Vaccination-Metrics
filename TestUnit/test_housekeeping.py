import pytest
import os
import sys
sys.path.insert(0, '../')
from application.housekeeping import housekeeping
housekeep = housekeeping()

def test_housekeeping():
    logger = housekeep.test_log()
    return_code = housekeep.job_Housekeeping(logger)
    assert return_code == 0
