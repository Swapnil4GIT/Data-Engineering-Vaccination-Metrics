from housekeeping import *
from datacleansing import *
from create_config import *
from datamerging import *
from datatransaggregate import *
from dataarchiveandpurge import * 

config = generate_config()

housekeep = housekeeping()
dataclean = datacleansing()
datamerge = datamerging()
dataaggre = datatransaggregate()
darchive = archive_and_purge()

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
                jb_arch_rc = darchive.job_archive_datasets(logger)
                if (jb_arch_rc):
                    logger.error("[ERROR]: Aborting the data archive and purge job.")
                    housekeep.abort()
                else:
                    logger.info("[INFO]: Data archive and purge job completed successfully.")