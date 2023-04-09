import pandas as pd 
import os
import sys 
sys.path.insert(0, '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Application')
from housekeeping import *

class datatransaggregate:
    
    IND_POPULATION = 1417422253 
    USA_POPULATION = 33190000
    AUS_POPULATION = 25700000

    population_dict = {'Country': ['IND', 'USA', 'AUS'],
                        'Population':[IND_POPULATION, USA_POPULATION, AUS_POPULATION]}
    population_df = pd.DataFrame(population_dict)

    def calculateMetric1(self, df):
        try:
            metric1 = df.groupby(['Country','VaccinationType']).agg(VaccinatedCount = pd.NamedAgg('ID', 'count')).reset_index()
            metric1.to_csv('../Staging/Metric1.csv', index=None)
            return 0
        except:
            return 1
        
    def calculateMetric2(self, df):
        try:
            metric2 = df.groupby(['Country']).agg(VaccinatedCount = pd.NamedAgg('ID', 'count')).reset_index()
            metric2 = metric2.merge(self.population_df, on='Country', how='inner')
            metric2['VaccinatedPopulationPercentage'] = metric2['VaccinatedCount']/metric2['Population']*100
            metric2['VaccinatedPopulationPercentage'] = metric2['VaccinatedPopulationPercentage'].round(10)
            metric2 = metric2.drop(['VaccinatedCount','Population'], axis=1)
            metric2.to_csv('../Staging/Metric2.csv', index=None)
            return 0
        except:
            return 1
    
    def calculateMetric3(self, df):
        try:
            metric3 = pd.read_csv('../Staging/Metric2.csv')
            metric3['TotalPercentage'] = metric3['VaccinatedPopulationPercentage'].sum()
            metric3['VaccinationContributionPercentage'] = metric3['VaccinatedPopulationPercentage']/metric3['TotalPercentage']*100
            metric3['VaccinationContributionPercentage'] = metric3['VaccinationContributionPercentage'].round(10)
            metric3 = metric3.drop(['VaccinatedPopulationPercentage','TotalPercentage'], axis=1)
            metric3.to_csv('../Staging/Metric3.csv', index=None)
            return 0
        except:
            return 1

    def job_Datatransaggregate(self, logger):
        logger.info("[INFO]: ----------------------------------------------------------")
        logger.info("[INFO]: Data transformation and aggregate job started.")

        try:
            df = pd.read_csv('../Staging/ALL.csv')
        except:
            return 1
        
        rc = self.calculateMetric1(df)
        if (rc):
            return 1
        
        rc = self.calculateMetric2(df)
        if (rc):
            return 1
        
        rc = self.calculateMetric3(df)
        if (rc):
            return 1

        return 0