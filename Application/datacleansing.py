import pandas as pd
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class datacleansing:
    
    def convert_Object_to_Date(self, obj):
        try:
            return pd.to_datetime(obj)
        except:
            return pd.to_datetime('1920-01-01')

    def commoncleansing(self, df, f):
        error_df = df.loc[df.isna().any(axis=1)]
        df = df.loc[~df.isna().any(axis=1)]
        df['VaccinationDate'] = df['VaccinationDate'].apply(lambda x: self.convert_Object_to_Date(x))
        df['VaccinationDate'] = df['VaccinationDate'].dt.date
        staging_file = '../Staging/' + f
        error_file = '../Error/' + f
        
        if f.endswith('.xlsx'):
            df.to_excel(staging_file, index=None)
            error_df.to_excel(error_file, index=None)
        else:
            df.to_csv(staging_file, index=None)
            error_df.to_csv(error_file, index=None)

    def datacleansing_IND(self, logger, fname, f):
        logger.info("[INFO]: Started cleaning the dataset for IND.csv.")
        df = pd.read_csv(fname)
        df['Country'] = 'IND'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        self.commoncleansing(df, f)

    def datacleansing_USA(self, logger, fname, f):
        df = pd.read_csv(fname)
        df['Country'] = 'USA'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        self.commoncleansing(df, f)

    def datacleansing_AUS(self, logger, fname, f):
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
        self.commoncleansing(df, f)

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
                self.datacleansing_IND(logger, fname, f)
            elif f in ['USA.csv', 'USA.CSV']:
                self.datacleansing_USA(logger, fname, f)
            elif f in ['AUS.xlsx', 'AUS.XLSX']:
                self.datacleansing_AUS(logger, fname, f)
            else:
                logger.warning("[WARNING]: New country's data is received. Please check.")
                return 2
        logger.info("[INFO]: Data cleansing job ended successfully.")    
        return 0
        