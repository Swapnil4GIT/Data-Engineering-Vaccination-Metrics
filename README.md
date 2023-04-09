# Data Engineering Vaccination Metrics
 Data Engineering task to report vaccination metrics

[Instructions to make changes on your local system before run:]
1. Please do not change the directory structure.
2. Please do not change the directory names.
3. Make appropriate changes to Config/configfile.ini. You need to change the directory paths of your choice.
4. Application/housekeeping.py ==> Change directory path in sys.path.insert() as per your need.
5. Application/datacleansing.py ==> Change directory path in sys.path.insert() as per your need.
6. Application/datamerging.py ==> Change directory path in sys.path.insert() as per your need.
7. Application/datatransaggregate.py ==> Change directory path in sys.path.insert() as per your need.
8. Application/datatransaggregate.py ==> Change the country population constants if needed.
9. Application/dataarchiveandpurge.py ==> Change the directory path in sys.path.insert() as per your need.
10. Application/dataarchiveandpurge.py ==> Change the src and tar path variables as per your need.
10. TestUnit/test_housekeeping.py ==> Change directory path in sys.path.insert() as per your need.
11. TestUnit/test_datacleansing.py ==> Change directory path in sys.path.insert() as per your need.
12. TestUnit/test_dataarchiveandpurge.py ==> Change the directory path in the sys.path.insert() as per your need.
13. TestUnit/test_datamerging.py ==> Change the directory path in the sys.path.insert() as per your need.
14. TestUnit/test_datatransaggregate.py ==> Change the directory path in the sys.path.insert() as per your need.
---------------------------------------------------------
[Instructions to execution the process on your local system:]
1. Open the repository in your VS code.
2. Open the terminal at the bottom in VS code.
3. cd to Application directory.
4. Exectue the job for full feed or delta feed depending the need.
5. Please make sure you execute at least one full feed run before running the delta. Else the process will abort.
6. Command to execute for processing the full feed: python main.py F
7. Command to execute for processing the delta feed: python main.py D
---------------------------------------------------------
[Directory structure and details:]
1. Application: Contains the .py code of all the jobs including the main.py.
2. Archive: Contains the archived files with date appended to the file name after successful processing.
3. Config: Contains the configfile.ini.
4. Data: Location of input dataset files.
5. DataModel: Contains screenshot of sample file layout format of all 3 countries datasets.
6. Error: Contains the datasets from the individual contries having records with missing values.
7. Logs: Contains the log files. ".log" for actual process and "_test.log" for pytest process.
8. SOW: Contains project statement. Statement Of Work.
9. Staging: Contains cleaned datasets, merged datasets and metrics created.
10. TestUnit: Contains all the pytest written for the different jobs.