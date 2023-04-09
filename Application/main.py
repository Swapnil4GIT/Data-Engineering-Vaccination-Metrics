from housekeeping import *
from datacleansing import *
from create_config import *
from datamerging import *
from datatransaggregate import *

config = generate_config()

housekeep = housekeeping()
dataclean = datacleansing()
datamerge = datamerging()
dataaggre = datatransaggregate()

logger = housekeep.init_log()
logger.setLevel(logging.DEBUG)

jb_hkeep_rc = housekeep.job_Housekeeping(logger)
if (jb_hkeep_rc):
    logger.error("[ERROR]: Aborting the housekeeping job.")
    housekeep.abort()
else:
    logger.info("[INFO]: Housekeeping job ended successfully.")
    jb_dclean_rc = dataclean.job_Datacleansing(logger)
    if (jb_dclean_rc):
        logger.error("[ERROR]: Aborting the datacleansing job.")
        housekeep.abort()
    else:
        logger.info("[INFO]: Datacleansing job ended successfully.")
        jb_dmerge_rc = datamerge.job_Datamerging(logger)
        if (jb_dmerge_rc):
            logger.error("[ERROR]: Aborting the datamerging job.")
            housekeep.abort()
        else:
            logger.info("[INFO]: Datamerging job ended successfully.")
            jb_daggre_rc = dataaggre.job_Datatransaggregate(logger)
            if (jb_daggre_rc):
                logger.error("[ERROR]: Aborting the data transformation and aggregate job.")
                housekeep.abort()
            else:
                logger.info("[INFO]: Data transformation and aggregate job ended successfully.")