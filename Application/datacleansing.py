import pandas as pd
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class datacleansing:
    
    def datacleansing_IND(self, logger, fname):
        df = pd.read_csv(fname)
        df['Country'] = 'IND'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        df.to_csv('../Staging/IND.csv', index=None)

    def datacleansing_USA(self, logger, fname):
        df = pd.read_csv(fname)
        df['Country'] = 'USA'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        df.to_csv('../Staging/USA.csv', index=None)

    def datacleansing_AUS(self, logger, fname):
        df = pd.read_excel(fname)
        df['Country'] = 'AUS'
        df = df.rename(columns={
            'Unique ID' : 'ID',
            'Patient Name' : 'Name',
            'Vaccine Type' : 'VaccinationType',
            'Date of Birth' : 'DOB',
            'Date of Vaccination' : 'VaccinationDate'
        })
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        df.to_csv('../Staging/AUS.csv', index=None)
        
    def job_Datacleansing(self, logger):
        housekeep = housekeeping()
        logger.info("[INFO]: ----------------------------------------------------------")
        logger.info("[INFO]: Data cleansing job started.")
        logger.info("[INFO]: Starting the data cleansing process.")
        filepath = housekeep.getSource_File_Path(logger)
        files = os.listdir(filepath)
        files = [f for f in files if not f.startswith('.')]
        for f in files:
            fname = filepath + f
            if f in ['IND.csv', 'IND.CSV']:
                self.datacleansing_IND(logger, fname)
            elif f in ['USA.csv', 'USA.CSV']:
                self.datacleansing_USA(logger, fname)
            elif f in ['AUS.xlsx', 'AUS.XLSX']:
                self.datacleansing_AUS(logger, fname)
            else:
                logger.warning("[WARNING]: New country's data is received. Please check.")
                return 2
        logger.info("[INFO]: Data cleansing job ended successfully.")    
        return 0
        