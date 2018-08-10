echo off

REM ### Point to the correct folder and script file
SET "SolutionDir=E:\Code\IMERG_Accumulations_ETL\IMERG_Accumulations_ETL.py"

REM ### Run the script with the desired command line option
C:\Python27\ArcGIS10.4\python.exe "%SolutionDir%" -l DEBUG
REM C:\Python27\ArcGISx6410.4\python.exe "%SolutionDir%" -l INFO

