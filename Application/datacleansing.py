import pandas as pd
import os
import sys
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class datacleansing:
    
    def IND_convert_Object_to_Date(self, obj):
        try:
            return datetime.strptime(str(obj), '%Y-%m-%d')
        except:
            return pd.to_datetime('1920-01-01')

    def USA_convert_Object_to_Date(self, obj):
        try:
            return datetime.strptime(str(obj).strip(), '%m%d%Y')
        except:
            return pd.to_datetime('1920-01-01')
    
    def AUS_convert_Object_to_Date(self, obj):
        try:
            return datetime.strptime(str(obj).strip(), '%Y-%m-%d')
        except:
            try:
                return datetime.strptime(str(obj).strip(), '%Y-%m-%d %H:%M:%S')
            except:
                return pd.to_datetime('1920-01-01')
        
    def commoncleansing(self, df, f):
        error_df = df.loc[df.isna().any(axis=1)]
        df = df.loc[~df.isna().any(axis=1)]
        
        if f.startswith('IND'):
            df['VaccinationDate'] = df['VaccinationDate'].apply(lambda x: self.IND_convert_Object_to_Date(x))
        elif f.startswith('USA'):
            df['VaccinationDate'] = df['VaccinationDate'].apply(lambda x: self.USA_convert_Object_to_Date(x))
        elif f.startswith('AUS'):
            df['VaccinationDate'] = df['VaccinationDate'].apply(lambda x: self.AUS_convert_Object_to_Date(x))

        df['VaccinationDate'] = df['VaccinationDate'].dt.date
        filename = os.path.splitext(f)[0]
        staging_file = '../Staging/' + filename + '.csv'
        error_file = '../Error/' + filename + '.csv'        
        df.to_csv(staging_file, index=None)
        error_df.to_csv(error_file, index=None)

    def datacleansing_IND(self, logger, fname, f):
        logger.info("[INFO]: Started data cleansing for IND.csv.")
        df = pd.read_csv(fname)
        df['Country'] = 'IND'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        df['VaccinationDate'] = df['VaccinationDate'].astype(str).apply(lambda x: x.strip())
        self.commoncleansing(df, f)
        logger.info("[INFO]: Ended successfully data cleansing for IND.csv.")

    def datacleansing_USA(self, logger, fname, f):
        logger.info("[INFO]: Started data cleansing for USA.csv.")
        df = pd.read_csv(fname)
        df['Country'] = 'USA'
        df = df[['ID', 'Country', 'Name', 'VaccinationType', 'VaccinationDate']]
        df['VaccinationDate'] = df['VaccinationDate'].astype(str).apply(lambda x: x.strip())
        self.commoncleansing(df, f)
        logger.info("[INFO]: Ended successfully data cleansing for USA.csv.")

    def datacleansing_AUS(self, logger, fname, f):
        logger.info("[INFO]: Started data cleansing for AUS.csv.")
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
        df['VaccinationDate'] = df['VaccinationDate'].astype(str).apply(lambda x: x.strip())
        self.commoncleansing(df, f)
        logger.info("[INFO]: Ended successfully data cleansing for AUS.csv.")

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
        