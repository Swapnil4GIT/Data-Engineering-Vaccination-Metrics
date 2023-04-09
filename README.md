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

[Instructions to execution the process on your local system:]
1. Open the repository in your VS code.
2. Open the terminal at the bottom in VS code.
3. cd to Application directory.
4. Exectue the job for full feed or delta feed depending the need.
5. Please make sure you execute at least one full feed run before running the delta. Else the process will abort.
6. Command to execute for processing the full feed: python main.py F
7. Command to execute for processing the delta feed: python main.py D