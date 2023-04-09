#----------------------------------------------------------------------------------------------------------#
# This is an archival and purge job. The job archives the input dataset files after successful process.    #
# The job also purges the old logs based on max log keep parameters threshold. Purge not yet coded.        #
# The job takes files from Data directory, adds date to the filename and copies to Archive directory.      #
# The job return_code = 0 for success and return_code = 1 for failure.                                     #
# The job is called from main.py                                                                           #
#----------------------------------------------------------------------------------------------------------#
import pandas as pd 
import os
import sys 
import shutil
import datetime 
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class archive_and_purge:

    src = '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Data/'
    tar = '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Archive/'

    def job_archive_datasets(self, logger):
        housekeep = housekeeping()
        logger.info("[INFO]: ----------------------------------------------------------")
        logger.info("[INFO]: Data archival and purge job started.")

        try:
            filepath = housekeep.getSource_File_Path(logger)
            files = os.listdir(filepath)
            files = [f for f in files if not f.startswith('.')]
            current_date = datetime.today().strftime ('%m-%d-%Y')

            for f in files:
                filename = os.path.splitext(f)[0]
                filename = filename + '_' + current_date + '.csv'
                srcfile = self.src + f 
                tarfile = self.tar + filename
                shutil.copy(srcfile, tarfile)
            return 0
        except:
            return 1
        
        return 0

    def job_purgeLogs(self, logger):
        pass