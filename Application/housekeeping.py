import os
import configparser
import pathlib
import logging
from datetime import datetime
import sys

class housekeeping:
    
    def getSource_File_Path(self, logger):
        logger.info("[INFO]: Reading configuration file.")
        conffilepath = '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Config/configfile.ini'
        configfile = pathlib.Path(conffilepath)
        
        if configfile.exists():
            logger.info("[INFO]: Configuration file: " + str(configfile))
            config_obj = configparser.ConfigParser()
            config_obj.read(configfile)
            try:
                srcfilepath = config_obj['parameters']['filepath']
                logger.info("[INFO]: Source file location: " + srcfilepath)
                return srcfilepath
            except KeyError as e:
                logger.error("[ERROR]: filepath parameter not found in the configfile.ini.")
                return 1
        else:
            logger.error("[ERROR]: configfile.ini is missing.")
            logger.error("[ERROR]: configfile.ini expected location: " + str(conffilepath))
            return 1
        
    def getLog_File_Path(self, logger):
        conffilepath = '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Config/configfile.ini'
        configfile = pathlib.Path(conffilepath)
        if configfile.exists():
            logger.info("[INFO]: Configuration file: " + str(configfile))
            config_obj = configparser.ConfigParser()
            config_obj.read(configfile)
            try:
                logpath = config_obj['parameters']['logpath']
                logger.info("[INFO]: Log file location: " + logpath)
                return logpath
            except KeyError as e:
                logger.error("[ERROR]: logpath parameter not found in the configfile.ini.")
                return 1
        else:
            logger.error("[ERROR]: configfile.ini is missing.")
            return 1
        

    def isValid_File_Type(self, f):
        if f.endswith('.xlsx') or f.endswith('.XLSX'):
            return True
        elif f.endswith('.csv') or f.endswith('.CSV'):
            return True
        elif f.startswith('.'):
            return True
        else:
            return False
        

    def init_log(self):
        logger=logging.getLogger()
        logpath = self.getLog_File_Path(logger)
        logfile = logpath + str(datetime.now().strftime('%Y_%m_%d')) + '.log'
        logging.basicConfig(filename=logfile,
                            format='%(asctime)s %(message)s',
                            filemode='w',
                            force=True)
        return logger


    def abort(self):
        sys.exit()


    def job_Housekeeping(self, logger):
        logger.info("[INFO]: ---------------------------------------------------------- ")
        logger.info("[INFO]: Housekeeping job started.")
        logger.info("[INFO]: Starting the full feed process.")
        
        filepath = self.getSource_File_Path(logger)
        
        if filepath == 1:
            return 1
        
        files = os.listdir(filepath)
        files = [f for f in files if not f.startswith('.')]
        if len(files) == 0:
            logger.error("[ERROR]: Input files are missing. Please contact the source system.")
            return 1

        logger.info("[INFO]: Validating the file types received from source.")
        for f in files:
            if self.isValid_File_Type(f):
                logger.info("[INFO]: Valid file type. Filename is: " + f)
            else:
                logger.error("[ERROR]: Invalid file type received from source. Filename is: " + f)
                return 1
        return 0