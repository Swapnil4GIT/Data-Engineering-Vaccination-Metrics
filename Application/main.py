from housekeeping import *
from datacleansing import *
from create_config import *

config = generate_config()

housekeep = housekeeping()
dataclean = datacleansing()

logger = housekeep.init_log()
logger.setLevel(logging.DEBUG)

print(sys.path)
jb_hkeep_rc = housekeep.job_Housekeeping(logger)
if (jb_hkeep_rc):
    logger.error("[ERROR]: Aborting the housekeeping job.")
    housekeep.abort()
else:
    logger.info("[INFO]: Housekeeping job ended successfully.")
    dataclean.job_Datacleansing(logger)