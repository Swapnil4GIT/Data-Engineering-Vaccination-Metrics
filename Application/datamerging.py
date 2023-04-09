import pandas as pd
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class datamerging:

    def job_Datamerging(self, logger):
        logger.info("[INFO]: ----------------------------------------------------------")
        logger.info("[INFO]: Data merging job started.")
        try:
            df_ind = pd.read_csv('../Staging/IND.csv')
            df_usa = pd.read_csv('../Staging/USA.csv')
            df_aus = pd.read_csv('../Staging/AUS.csv')

            df_all = pd.concat([df_ind, df_usa, df_aus], ignore_index=True, sort=False)
            df_all.to_csv('../Staging/ALL.csv')
            return 0
        except:
            return 1
        
        return 0
