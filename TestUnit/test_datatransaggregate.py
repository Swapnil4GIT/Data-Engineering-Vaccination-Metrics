import pytest
import os
import sys
from typing import Callable, NoReturn
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics')
from Application.housekeeping import housekeeping
from Application.datatransaggregate import datatransaggregate

housekeep = housekeeping()
logger = housekeep.test_log()
daggre = datatransaggregate()

def test_datatransaggregate_rc_1():
    return_code = daggre.job_Datatransaggregate(logger)
    assert return_code in [0]

