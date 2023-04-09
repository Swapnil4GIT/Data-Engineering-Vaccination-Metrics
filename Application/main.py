#----------------------------------------------------------------------------------------------------------#
# This is the main driver job. The main job calls the individual jobs in below order -                     #
# 1. job_Housekeeping                                                                                      #
# 2. job_Datacleansing                                                                                     #
# 3. job_Datamerging                                                                                       #
# 4. job_Datatransaggregate                                                                                #
# 5. job_archive_and_purge                                                                                 # 
#----------------------------------------------------------------------------------------------------------#

from housekeeping import *
from datacleansing import *
from create_config import *
from datamerging import *
from datatransaggregate import *
from dataarchiveandpurge import * 

def mainFunction():

    args = sys.argv[1:]
    
    config = generate_config()

    housekeep = housekeeping()
    dataclean = datacleansing()
    datamerge = datamerging()
    dataaggre = datatransaggregate()
    darchive = archive_and_purge()

    logger = housekeep.init_log()
    logger.setLevel(logging.DEBUG)

    if len(args) == 0:
        logger.error("[ERROR]: Full or Delta feed paramter missing. Aborting..")
        housekeep.abort()
    elif len(args) > 0:
        if args[0] == 'F':
            logger.info("[INFO]: THIS IS FULL FEED PROCESSING.")
        elif args[0] == 'D':
            logger.info("[INFO]: THIS IS DELTA FEED PROCESSING.")
        else:
            logger.error("[ERROR]: INVALID PARAMETER PASSED TO THE MAIN PROCESS. Aborting..")
            housekeep.abort()
    else:
        logger.error("[ERROR]: Some issue with the args parameters. Aborting..")
        housekeep.abort()

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
            if args[0] == 'F':
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
            elif args[0]=='D':
                jb_dmerge_rc = datamerge.job_delta_Datamerging(logger)
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

if __name__ == "__main__":
    mainFunction()
