<a href="https://www.servirglobal.net//">
    <img src="https://www.servirglobal.net/Portals/0/Images/Servir-logo.png" alt="SERVIR Global"
         title="SERVIR Global" align="right" />
</a>


IMERG Accumulations ETL
=======================
> Python script for automating the Extract, Transform, and Load of raster data from a source ftp location into a file geodatabase mosaic datasets (shared as image services)

## Introduction:
This IMERG Accumulations ETL (Extract, Transform, and Load) retrieves the latest 1, 3, and 7 Day IMERG (Integrated Multi-satellitE Retrievals for GPM) precipitation data from the [NASA/Goddard Space Flight Center's GPM Team and Precipitation Processing System (PPS)](http://pmm.nasa.gov/GPM) ftp server and processes/loads the data into a file geodatabase (mosaic datasets) supporting respective [1 Day](https://gis1.servirglobal.net/arcgis/rest/services/Test/IMERG1Day_ImgSvc/ImageServer), [3 Day](https://gis1.servirglobal.net/arcgis/rest/services/Test/IMERG3Day_ImgSvc/ImageServer), and [7 Day](https://gis1.servirglobal.net/arcgis/rest/services/Test/IMERG7Day_ImgSvc/ImageServer) Image Services.  The source 1 Day, 3 Day, and 7 Day tif files are generated daily (along with other available products) and placed on the ftp site (currently jsimpson.pps.eosdis.nasa.gov - please see [https://pmm.nasa.gov/data-access](https://pmm.nasa.gov/data-access) for more information about accessing the data products).

## Details: 
The high-level processing details are:
1. Connect to the source ftp site and change to the proper folder that contains the files that we want.
2. Get the list of filenames from the ftp folder. Process through the list and identify which specific files (1 Day, 3 Day, and 7 Day) we are interested in downloading.
3. Download only the files that we need into a temporary extract folder.
4. Process through the files in the temp extract folder and a.) rewrite/save the each file to it's proper final folder location as the desired filename, and b.) load each file to it's respective file geodatabase mosaic dataset.
5. As each file is processed successfully, delete the temp extract copy of the file.
6. Compact the file geodatabase.
7. Refresh (Stop and Restart) each of the services. (1, 3, and 7 Day)

As the source ftp files are generated in a folder hierarchy broken down by ../(basefolder)/(year)/(month), this script uses the current date to determine the source ftp folder location and then downloads the latest 1, 3, and 7 day files based on the date/time stamp in the file names.  (The files are named similar to '3B-HHR-L.MS.MRG.3IMERG.20180809-S233000-E235959.1410.V05B.1day.tif' and the code logic parses out the date/start time from the filename string to determine the latest files.)  Once the most recent files are downloaded to a temp extract folder, the script then processes each file in that folder and extracts only pixel values > 0 and < 29990 and saves the resulting files into the source folder supporting the mosaic datasets. As the files are extracted, they are renamed to IMERG1Day.tif, IMERG3Day.tif, and IMERG7Day.tif before being loaded into their respective mosaic dataset.  (Each mosaic dataset will only ever contain 1 raster entry - which is overwritten each time a new file is loaded.)  As each downloaded file is loaded into it's mosaic dataset and copied into the folder supporting the mosaic dataset, the downloaded file is deleted from the temp extract folder.  Finally, the corresponding ArcGIS Image service is stopped and restarted to reflect the added data.

## Environment:
IMERG_Accumulations_ETL.py is the main script file and was created and tested with python 2.7. The script relies on Esri's Arcpy module, as well as their Spatial Analyst extension for the arcpy.sa.ExtractByAttributes() method.  The tif files are loaded into raster mosaic datasets within an Esri file geodatabase.  The file geodatabase and the mosaic datasets can be located and named whatever you want - these settings are ultimately stored in the config.pkl file.

The IMERG_Accumulations_Pickle.py file contains a dictionary object with the needed configuration parameters and is used to generate a configuration file (config.pkl) that is read by the main script at run time.  Please carefully modify the paths and username/password variables in IMERG_Accumulations_Pickle.py to meet your needs!  IMERG_Accumulations_Pickle.bat is simply a batch file to run the IMERG_Accumulations_Pickle.py file to generate config.pkl.

Below are the configuration settings that are stored in the pickle file and their description:
```
      'extract_AccumulationsFolder':    Local folder where the ftp files will be downloaded.
      'final_Folder':                   Local source folder supporting the mosaic datasets. This is where the downloaded files will ultimately reside once loaded into the mosaic.
      'logFileDir':                     Local folder where the log file will be written.
      'logFilePrefix':                  Prefix/Name for the log file.  i.e. 'IMERG_Accumulations'
      'GDBPath':                        Path and filename for the file geodatabase.  i.e. 'C:/somefolder/myFileGeodatabase.gdb'
      '1DayDSName':                     Name of the mosaic dataset for the 1 Day IMERG data.  i.e. 'IMERG1Day'
      '3DayDSName':                     Name of the mosaic dataset for the 3 Day IMERG data.  i.e. 'IMERG3Day'
      '7DayDSName':                     Name of the mosaic dataset for the 7 Day IMERG data.  i.e. 'IMERG7Day'
      'rasterStartTimeProperty':        Name of the field in the mosaic dataset that will receive the start date/time value.  i.e. 'start_datetime'
      'rasterEndTimeProperty':          Name of the field in the mosaic dataset that will receive the end date/time value.  i.e. 'end_datetime'
      'RegEx_StartDateFilterString':    A regular expression format string that helps identify the date and start timestamp portion within the IMERG filenames.  i.e. '\d{4}[01]\d[0-3]\d-S[0-2]\d{5}'
      'GDB_DateFormat':                 A format string for dates.  i.e. '%Y%m%d%H%M'
      'Filename_StartDateFormat':       A format string that helps identify the date and start timestamp portion within the IMERG filenames.  i.e. '%Y%m%d-S%H%M%S'
      'ftp_host':                       The name of the ftp site for downloading IMERG data.  i.e. 'jsimpson.pps.eosdis.nasa.gov'
      'ftp_user':                       ftp site USERNAME
      'ftp_pswrd':                      ftp site PASSWORD
      'ftp_baseLateFolder':             ftp site base folder for where we will retrieve the 1, 3, and 7 day files.  i.e. '/data/imerg/gis'
      'svc_adminURL':                   Base ArcGIS Admin URL for your Image Services. i.e. 'https://gis1.servirglobal.net/arcgis/admin'
      'svc_username':                   ArcGIS Admin USERNAME
      'svc_password':                   ArcGIS Admin PASSWORD
      'svc_folder':                     Name of folder where the Image Services reside. i.e. 'Test' or 'Global' or '#' (if in root).
      'svc_Name_1Day':                  Name of the 1 Day Image Service
      'svc_Name_3Day':                  Name of the 3 Day Image Service
      'svc_Name_7Day':                  Name of the 7 Day Image Service
      'svc_Name_All':                   Name of the Map Service containing all 3 of the Accumulation layers
      'JSONFile_ServiceUpdates':        Path and filename of a SERIVR-specific JSON file that tracks the datetime stamp and service name that is updated.  i.e. 'C:\inetpub\wwwroot\SERVIRservices.json'
```

## Prerequisites:
 * The user running the script must be allowed folder and file write permissions on the local machine.
 * The file geodatabase and the associated mosaic datasets must already exist.
 * The GPM/PPS ftp account (username and password) must already be established.
 * The associated ArcGIS image services must already exist with proper admin account credentials required for managing the services.

## Instructions to prep the script for running:
1.	Go to IMERG_Accumulations_Pickle.py and CAREFULLY enter your specific paths and credentials.
2.  Go to IMERG_Accumulations_Pickle.bat and a.) check the path to your version of python.exe, and b.) update the path to your copy of IMERG_Accumulations_Pickle.py.
3.  Run IMERG_Accumulations_Pickle.bat to generate the 'config.pkl' settings file in the same folder.  (config.pkl file is required for the main script.)
4.	Go to IMERG_Accumulations_ETL.bat and a.) check the path to your version of python.exe, and b.) update the path to your copy of IMERG_Accumulations_ETL.py.
5.  Run IMERG_Accumulations_ETL.bat to execute the main script.

