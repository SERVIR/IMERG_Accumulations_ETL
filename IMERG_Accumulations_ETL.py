# -------------------------------------------------------------------------------
# Name:        IMERG_Accumulations_ETL.py
# Purpose:     Retrieve the latest IMERG 1, 3, and 7 Day Accumulation files from their source FTP location and
#               load them into a file geodatabase raster mosaic dataset as IMERG1Day.tif, IMERG3Day.tif,
#               and IMERG7Day.tif, respectively. (Overwriting the previous entries. Each file has it's own
#               mosaic dataset in the file GDB.)
#
# Author:               Lance Gilliland / SERVIR GIT Team       2018
# Last Modified By:
# Copyright:   (c) SERVIR 2018
#
# Note: This is a rewrite of the initial IMERG ETL - some portions of the initial code were reused.
# -------------------------------------------------------------------------------

import arcpy
import argparse  # required for processing command line arguments
import datetime
import time
import os

import pickle
import logging

import linecache  # required for capture_exception()
import sys  # required for capture_exception()

import re  # required for Regular Expressions

import urllib  # required for RefreshService() (stopping and starting services) and retrieving remote files.
import urllib2  # required for retrieving remote files.

import ftplib  # require for ftp downloads
import json  # required for UpdateServicesJsonFile() (updating services JSON file)


# ------------------------------------------------------------
# Read configuration settings
# Global Variables - contents will not change during execution
# ------------------------------------------------------------
pkl_file = open('config.pkl', 'rb')
myConfig = pickle.load(pkl_file)
pkl_file.close()


class RasterLoadObject(object):
    """
        A class to hold information about a raster that is to be added to a mosaic dataset.
        Normally, a raster is loaded simply using it's filename, but in this case, we will be loading
        rasters with different filenames into the mosaic dataset as a common name - overwriting the existing
        raster entry each time. This is why we have both the origFile and loadFile properties.
    """

    def __init__(self, oFile="default", lFile="default", sDate=None, eDate=None, sDataset="default"):
        self.origFile = oFile
        self.loadFile = lFile
        self.startDate = sDate
        self.endDate = eDate
        self.targetDataset = sDataset

    def origFile(self, oFile):
        self.origFile = oFile

    def loadFile(self, lFile):
        self.loadFile = lFile

    def startDate(self, sDate):
        self.startDate = sDate

    def endDate(self, eDate):
        self.endDate = eDate

    def targetDataset(self, sDataset):
        self.targetDataset = sDataset


class MapService(object):
    """
        A class to hold information about a map or image service.  i.e.
          'adminURL': 'https://gis1.servirglobal.net/arcgis/admin',
          'username': 'someVal',
          'password': 'someVal',
          'folder': 'Global',
          'svcName': 'IMERG_Accumulation_ImgSvc',
          'svcType': 'ImageServer'}
    """

    def __init__(self, url="", uname="", psswd="", fldr="", svc_name="", svc_type=""):
        self.adminURL = url
        self.username = uname
        self.password = psswd
        self.folder = fldr
        self.svcName = svc_name
        self.svcType = svc_type

    def adminURL(self, url):
        self.adminURL = url

    def username(self, uname):
        self.username = uname

    def password(self, psswd):
        self.password = psswd

    def folder(self, fldr):
        self.folder = fldr

    def svcName(self, svc_name):
        self.svcName = svc_name

    def svcType(self, svc_type):
        self.svcType = svc_type


def setupArgs():
    # Setup the argparser to capture any arguments...
    parser = argparse.ArgumentParser(__file__,
                                     description="This is the ETL script for the GPM IMERG 30 Minute dataset!")
    # Optional argument
    parser.add_argument("-l", "--logging",
                        help="the logging level at which the script should report",
                        type=str, choices=['debug', 'DEBUG', 'info', 'INFO', 'warning', 'WARNING', 'error', 'ERROR'])
    return parser.parse_args()


# Common function used by many!!
def capture_exception():
    # Not clear on why "exc_type" has to be in this line - but it does...
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    s = '### ERROR ### [{}, LINE {} "{}"]: {}'.format(filename, lineno, line.strip(), exc_obj)
    return s


def getScriptPath():
    # Returns the path where this script is running
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def getScriptName():
    # Tries to get the name of the script being executed...  returns "" if not found...
    try:
        # Get the name of this script!
        scriptFullPath = sys.argv[0]
        if len(scriptFullPath) < 1:
            return ""
        else:
            # In case it is the full pathname, split it...
            scriptPath, scriptLongName = os.path.split(scriptFullPath)
            # Split again to separate extension...
            scriptName, scriptExt = os.path.splitext(scriptLongName)
            return scriptName

    except:
        return ""


# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % seconds
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)


# Get a new time object
def get_NewStart_Time():
    timeStart = time.time()
    return timeStart


# Get the amount of time elapsed from the input time.
def get_Elapsed_Time_As_String(timeInput):
    return timeElapsed(timeInput)


def GetConfigString(variable):
    try:
        global myConfig
        return myConfig[variable]
    except:
        logging.error("### ERROR ###: Config variable NOT FOUND: {0}".format(variable))
        return ""


def create_folder(thePath):
    # Creates a directory on the file system if it does not already exist.
    # Then checks to see if the folder exists.
    # Returns True if the folder exists, returns False if it does not.
    try:
        # Create a location for the file if it does not exist..
        if not os.path.exists(thePath):
            os.makedirs(thePath)
        # Return the status
        return os.path.exists(thePath)
    except:
        return False


def ValidAccumulationRaster(fileName):
    """
        Accepts a filename and checks to see if it contains any of a few desired strings that identify the file
        as a desired file.
    """
    try:
        bRetVal = False
        if ".1day.tif" in fileName:
            bRetVal = True
        elif ".3day.tif" in fileName:
            bRetVal = True
        elif ".7day.tif" in fileName:
            bRetVal = True

        return bRetVal
    except:
        return False


def UpdateServicesJsonFile(jFile, serviceName, odateUpdated):
    """
    Read the json file and update the lastUpdated for the specified svcName.
    jFile should be the full path and filename for the json file.
    """
    try:

        # Convert the date object passed in to a formatted string
        sdateUpdated = odateUpdated.strftime('%Y-%m-%d %H:%M:%S')

        if os.path.isfile(jFile):

            # Open and read the file
            with open(jFile, "r") as jf:
                data = json.load(jf)
            jf.close()

            # Update the desired service info
            bUpdated = False
            for svc in data["Services"]:
                if svc["svcName"] == serviceName:
                    svc["lastUpdated"] = sdateUpdated
                    bUpdated = True
                    break

            # Check to see if anything was updated...
            if not bUpdated:
                # The service name didn't exist, so lets add it.
                data["Services"].append(
                    {
                        "svcName": serviceName,
                        "lastUpdated": sdateUpdated
                    }
                )

            # Open and write the file
            with open(jFile, "w") as f:
                json.dump(data, f)
            f.close()

        else:
            logging.info("JSON file for tracking services updates not found: {0}".format(jFile))

    except:
        logging.warning("Error updating Services JSON file with last updated date...")
        err = capture_exception()
        logging.error(err)


def Get_StartDateTime_FromString(theString, regExp_Pattern, source_dateFormat):
    """
        Search a string (or filename) for a date by using the regular expression pattern passed in, then use the
        date format passed in (which matches the filename date format) to convert the regular expression output
        into a datetime. Return None if any step fails.
    """
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern, theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            # If needed, this is where to insert a log entry or other notification that no date was found.
            return None
        else:
            # Found a string similar to:  20150802-S083000
            sExpStr = reItemsList[0]
            # Get a datetime object using the format from the filename.
            # The source_dateFormat should be a string similar to '%Y%m%d-S%H%M%S'
            dateObj = datetime.datetime.strptime(sExpStr, source_dateFormat)
            return dateObj
    except:
        return None


def GetLatestIMERGFileFromList(theFilenameList):
    """
        Accepts a list of filenames containing a date and start and end time string in a particular format.
        For instance:
            3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.1day.tif
        Particularly, we are interested in the date and start time string portion:  i.e.  20150802-S083000
        The goal is to identify and return the filename with the latest datetime.
    """
    try:
        # Grab a few needed settings.
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")

        # Set the initial latest date to some old date...
        slatestFileName = ""
        olatestDate = datetime.datetime.strptime("19500101", "%Y%m%d")  # Some random older date

        # Now, loop through the list and capture which one is the latest
        for tmpFile in theFilenameList:
            # Ex. filename format: 3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.1day.tif
            # If the item's date and start time is later than the olatestDate, we want to keep it.
            fileDate = Get_StartDateTime_FromString(tmpFile, RegEx_StartDatePattern, Filename_StartDateFormat)
            if (fileDate is not None) and (fileDate > olatestDate):
                olatestDate = fileDate
                slatestFileName = tmpFile

        # Return the status
        return slatestFileName

    except:
        return None


#  --- NOTE! NOTE! NOTE! ---
# For some unknown reason, our server (where this script will be running) cannot connect to the FTP site where we need
# to download files from. So, a "proxy" server/location has been established to retrieve the files from the FTP site.
# For this reason, we have implemented another function further below that uses URLLIB to retrieve the files from the
# proxy location vs. this function that uses FTPLIB to retrieve the files from the ftp location.
#  --- NOTE! NOTE! NOTE! ---
def ProcessAccumulationFiles(oTodaysDateTime):
    """
        Connects to the FTP site (base folder) and based on today's date, looks in a particular folder
        (year and month) and retrieves a list of .tif files.

        Note:  Files on the FTP site are broken down into folders by Year and then by Month.
            Late files are in FTP folder hierarchy:     /data/imerg/gis/<year>/<month>
                e.g  /data/imerg/gis/2017/01 ... /data/imerg/gis/2017/02 ... /data/imerg/gis/2018/01 ...
            So we must use Today's Date passed in to know which FTP folder we need to go to get the files.

        As it processes through the files in the folder, the list of potential filenames to download is reduced to only
        files that we want to keep by omitting any files that do not contain ".1day.tif", ".3day.tif", or ".7day.tif".
        As we find files with these names, they are placed in respective lists. Once the list of respectively named files
        is complete, the lists are searched for the latest file and the latest 1, 3, and 7 day files are downloaded to
        the proper extract location.
    """
    try:
        ftp_Host = GetConfigString("ftp_host")
        ftp_baseLateFolder = GetConfigString("ftp_baseLateFolder")
        ftp_UserName = GetConfigString("ftp_user")
        ftp_UserPass = GetConfigString("ftp_pswrd")

        targetFolder = GetConfigString("extract_AccumulationsFolder")

        # Set up the FTP connection
        bConnectionCreated = False
        ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        bConnectionCreated = True

        # Get the year and month from each date passed in
        oFolderYear = oTodaysDateTime.year
        oFolderMonth = oTodaysDateTime.month

        # Change to the <baseFolder>/Year/Month FTP folder
        sYear = str(oFolderYear)
        sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
        ftpFolder = ftp_baseLateFolder + "/" + sYear + "/" + sMonth
        ftp_Connection.cwd(ftpFolder)

        # Initialize a list for names of ALL files that are in the FTP folder
        tmpList = []
        # Initialize a placeholder list for names of files that we ACTUALLY process/download
        OneDayList = []
        ThreeDayList = []
        SevenDayList = []

        # Grab the list of ALL filenames from the current FTP folder...
        line = ftp_Connection.retrlines("NLST", tmpList.append)

        # Loop through each item in the tmpList and add it to the respective list!
        # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
        # To keep a file, it must contain the string ".1day.tif", ".3day.tif", or ".7day.tif"
        for ftpFile in tmpList:
            if ".1day.tif" in ftpFile:
                OneDayList.append(ftpFile)
            elif ".3day.tif" in ftpFile:
                ThreeDayList.append(ftpFile)
            elif ".7day.tif" in ftpFile:
                SevenDayList.append(ftpFile)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestOneDayFile = GetLatestIMERGFileFromList(OneDayList)
        if len(slatestOneDayFile) > 0:
            # Download the file to the targetFolder
            target1DayExtractFile = os.path.join(targetFolder, slatestOneDayFile)
            logging.info("Downloading latest 1Day file as {0}".format(target1DayExtractFile))
            with open(target1DayExtractFile, "wb") as f:
                ftp_Connection.retrbinary("RETR %s" % slatestOneDayFile, f.write)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestThreeDayFile = GetLatestIMERGFileFromList(ThreeDayList)
        if len(slatestThreeDayFile) > 0:
            # Download the file to the targetFolder
            target3DayExtractFile = os.path.join(targetFolder, slatestThreeDayFile)
            logging.info("Downloading latest 3Day file: {0}.".format(target3DayExtractFile))
            with open(target3DayExtractFile, "wb") as f:
                ftp_Connection.retrbinary("RETR %s" % slatestThreeDayFile, f.write)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestSevenDayFile = GetLatestIMERGFileFromList(SevenDayList)
        if len(slatestSevenDayFile) > 0:
            # Download the file to the targetFolder
            target7DayExtractFile = os.path.join(targetFolder, slatestSevenDayFile)
            logging.info("Downloading latest 7Day file: {0}.".format(target7DayExtractFile))
            with open(target7DayExtractFile, "wb") as f:
                ftp_Connection.retrbinary("RETR %s" % slatestSevenDayFile, f.write)

        # Delete the temp lists of filenames
        del OneDayList[:]
        del ThreeDayList[:]
        del SevenDayList[:]
        del tmpList[:]

        # Disconnect from ftp
        ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        if bConnectionCreated:
            ftp_Connection.close()
        return False


#  --- NOTE! NOTE! NOTE! ---
# This function is a replacement for ProcessAccumulationFiles() above. We cannot rely on FTP functionality, so we
# are using a proxy server that provides access to the needed ftp files via URLLIB functionality.
#  --- NOTE! NOTE! NOTE! ---
def ProcessAccumulationFiles_FromProxy(oTodaysDateTime):
    """
        Connects to the Proxy site (via URLLIB) and based on today's date, grabs filenames from a particular folder
        (year and month) and retrieves a list of .tif files.

        Note:  Source files on the FTP site are broken down into folders by Year and then by Month.
            Late files are in FTP folder hierarchy:     /data/imerg/gis/<year>/<month>
                e.g  /data/imerg/gis/2017/01 ... /data/imerg/gis/2017/02 ... /data/imerg/gis/2018/01 ...
            So we must use Today's Date passed in to know which FTP folder we need to go to get the files.

        As it processes through the files in the folder, the list of potential filenames to download is reduced to only
        files that we want to keep by omitting any files that do not contain ".1day.tif", ".3day.tif", or ".7day.tif".
        As we find files with these names, they are placed in respective lists. Once the list of respectively named files
        is complete, the lists are searched for the latest file and the latest 1, 3, and 7 day files are downloaded to
        the proper extract location.
    """
    try:
        # When using the proxy, we have to specify "ftp://" as part of the host string
        ftpHost = "ftp://" + GetConfigString("ftp_host")
        ftp_baseLateFolder = GetConfigString("ftp_baseLateFolder")
        targetFolder = GetConfigString("extract_AccumulationsFolder")

        # Set up the FTP connection
        # bConnectionCreated = False
        # ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        # bConnectionCreated = True

        # Get the year and month from each date passed in
        oFolderYear = oTodaysDateTime.year
        oFolderMonth = oTodaysDateTime.month

        # Change to the <baseFolder>/Year/Month FTP folder
        sYear = str(oFolderYear)
        sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
        ftpFolder = ftp_baseLateFolder + "/" + sYear + "/" + sMonth
        proxyDir = "https://proxy.servirglobal.net/ProxyFTP.aspx?directory="
        logging.debug("FTPProxy Directory URL = {0}".format(proxyDir + ftpHost + ftpFolder + "/"))
        # ftp_Connection.cwd(ftpFolder)
        req = urllib2.Request(proxyDir + ftpHost + ftpFolder + "/")   # last slash is required
        response = urllib2.urlopen(req)

        # Initialize a placeholder list for names of files that we ACTUALLY process/download
        OneDayList = []
        ThreeDayList = []
        SevenDayList = []

        # Grab the list of ALL filenames from the current FTP folder...
        # line = ftp_Connection.retrlines("NLST", tmpList.append)
        tmpList = response.read().split(",")

        # Loop through each item in the tmpList and add it to the respective list!
        # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
        # To keep a file, it must contain the string ".1day.tif", ".3day.tif", or ".7day.tif"
        for ftpFile in tmpList:
            if ".1day.tif" in ftpFile:
                OneDayList.append(ftpFile)
            elif ".3day.tif" in ftpFile:
                ThreeDayList.append(ftpFile)
            elif ".7day.tif" in ftpFile:
                SevenDayList.append(ftpFile)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestOneDayFile = GetLatestIMERGFileFromList(OneDayList)
        if len(slatestOneDayFile) > 0:
            # Download the source file to the targetFolder
            source1DayExtractFile = ftpHost + os.path.join(ftpFolder, slatestOneDayFile)
            target1DayExtractFile = os.path.join(targetFolder, slatestOneDayFile)
            logging.info("Downloading latest 1Day file: {0}".format(target1DayExtractFile))
            # with open(target1DayExtractFile, "wb") as f:
            #     ftp_Connection.retrbinary("RETR %s" % slatestOneDayFile, f.write)
            fx = open(target1DayExtractFile, "wb")
            fx.close()
            os.chmod(target1DayExtractFile, 0777)
            try:
                urllib.urlretrieve("https://proxy.servirglobal.net/ProxyFTP.aspx?url=" + source1DayExtractFile,
                                   target1DayExtractFile)
            except:
                logging.info("Error retrieving latest 1Day file from proxy: {0}".format(source1DayExtractFile))
                os.remove(target1DayExtractFile)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestThreeDayFile = GetLatestIMERGFileFromList(ThreeDayList)
        if len(slatestThreeDayFile) > 0:
            # Download the source file to the targetFolder
            source3DayExtractFile = ftpHost + os.path.join(ftpFolder, slatestThreeDayFile)
            target3DayExtractFile = os.path.join(targetFolder, slatestThreeDayFile)
            logging.info("Downloading latest 3Day file: {0}".format(target3DayExtractFile))
            # with open(target3DayExtractFile, "wb") as f:
            #     ftp_Connection.retrbinary("RETR %s" % slatestThreeDayFile, f.write)
            fx = open(target3DayExtractFile, "wb")
            fx.close()
            os.chmod(target3DayExtractFile, 0777)
            try:
                urllib.urlretrieve("https://proxy.servirglobal.net/ProxyFTP.aspx?url=" + source3DayExtractFile,
                                   target3DayExtractFile)
            except:
                logging.info("Error retrieving latest 3Day file from proxy: {0}".format(source3DayExtractFile))
                os.remove(target3DayExtractFile)

        # Find the latest file in the list based on the date and start time string in the filename
        slatestSevenDayFile = GetLatestIMERGFileFromList(SevenDayList)
        if len(slatestSevenDayFile) > 0:
            # Download the source file to the targetFolder
            source7DayExtractFile = ftpHost + os.path.join(ftpFolder, slatestSevenDayFile)
            target7DayExtractFile = os.path.join(targetFolder, slatestSevenDayFile)
            logging.info("Downloading latest 7Day file: {0}".format(target7DayExtractFile))
            # with open(target7DayExtractFile, "wb") as f:
            #     ftp_Connection.retrbinary("RETR %s" % slatestSevenDayFile, f.write)
            fx = open(target7DayExtractFile, "wb")
            fx.close()
            os.chmod(target7DayExtractFile, 0777)
            try:
                urllib.urlretrieve("https://proxy.servirglobal.net/ProxyFTP.aspx?url=" + source7DayExtractFile,
                                   target7DayExtractFile)
            except:
                logging.info("Error retrieving latest 3Day file from proxy: {0}".format(source7DayExtractFile))
                os.remove(target7DayExtractFile)

        # Delete the temp lists of filenames
        del OneDayList[:]
        del ThreeDayList[:]
        del SevenDayList[:]
        del tmpList[:]

        # Disconnect from ftp
        # ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        # if bConnectionCreated:
        #     ftp_Connection.close()
        return False


def LoadAccumulationRasters(temp_workspace):
    """
        This function accepts a temp workspace (folder) and:
        1 - Grabs each raster file (1day, 3day, and 7day) in the workspace folder and saves info from each one - storing
            the info into a list of class objects.
        2 - processes through the list of stored raster objects and extracts each raster from the temp folder into
            the final mosaic dataset folder(as it's respective file name), and then loads the raster to the mosaic
            dataset - overwriting any previous entries.
        3 - Uses info from the original source file to populate the start time and end time on each raster after
            it is loaded to the mosaic dataset.
        4 - deletes the temp_workspace original raster file after it is successfully added/moved to the mosaic dataset.
    """
    try:
        arcpy.CheckOutExtension("Spatial")
        # inSQLClause = "VALUE >= 0"
        # We do not want the zero values and we also do not want the "NoData" value of 29999.
        # So let's extract only the values above 0 and less than 29900.
        inSQLClause = "VALUE > 0 AND VALUE < 29900"
        arcpy.env.workspace = temp_workspace
        arcpy.env.overwriteOutput = True

        # Grab some config settings that will be needed...
        final_RasterSourceFolder = GetConfigString('final_Folder')
        target_mosaic1Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("1DayDSName"))
        target_mosaic3Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("3DayDSName"))
        target_mosaic7Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("7DayDSName"))
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")

        # Build attribute name list for updates
        attrNameList = [GetConfigString('rasterStartTimeProperty'), GetConfigString('rasterEndTimeProperty')]

        rasObjList = []

        # List all raster in the temp_workspace
        rasters = arcpy.ListRasters()
        for raster in rasters:

            # Check to see if this is a valid 1, 3, or 7 day raster file...  just in case there are other files
            if ValidAccumulationRaster(raster):

                keyDate = Get_StartDateTime_FromString(raster, RegEx_StartDatePattern, Filename_StartDateFormat)
                if keyDate is not None:

                    # Start deriving info (start_datetime, end_datetime, and target datastet) from the raster
                    # being processed. Build a 'raster load object' to hold the information about each raster
                    # that we need to keep track of.
                    rasLoadObj = RasterLoadObject()

                    # 1.) save the original file name
                    rasLoadObj.origFile = raster

                    # 2.) the date and start time portion (20180801-S083000) of the raster filename is used to set
                    #     the end_datetime attribute value on the loaded raster.  Save that here...
                    rasLoadObj.endDate = keyDate

                    # From the filename: ex. 3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.1day.tif
                    # 3.) the target dataset can be identified based on the occurrence of a string sequence
                    #     ".1day.tif" present means the target is the 1DayDSName
                    #     ".3day.tif" present means the target is the 3DayDSName
                    #     ".7day.tif" present means the target is the 7DayDSName
                    # 4.) the raster load file can also be set based on the occurrence of a string sequence
                    # 5.) the start_datetime attribute value is calculated based on the end_datetime and depending
                    #     on whether we are processing a 1, 3, or 7 day file, the start_datetime will be
                    #     calculated by subtracting the proper amount of days from the end_datetime.
                    if ".1day.tif" in raster:
                        rasLoadObj.targetDataset = target_mosaic1Day
                        rasLoadObj.loadFile = "IMERG1Day.tif"
                        rasLoadObj.startDate = keyDate - datetime.timedelta(days=1)
                    elif ".3day.tif" in raster:
                        rasLoadObj.targetDataset = target_mosaic3Day
                        rasLoadObj.loadFile = "IMERG3Day.tif"
                        rasLoadObj.startDate = keyDate - datetime.timedelta(days=3)
                    elif ".7day.tif" in raster:
                        rasLoadObj.targetDataset = target_mosaic7Day
                        rasLoadObj.loadFile = "IMERG7Day.tif"
                        rasLoadObj.startDate = keyDate - datetime.timedelta(days=7)

                    # At this point, we have built a raster load object that we can use later, add it to
                    # a list and continue looping through the rasters.
                    rasObjList.append(rasLoadObj)

        del rasters

        logging.info('Loading {0} raster files to folder {1}'.format(str(len(rasObjList)), final_RasterSourceFolder))
        for rasterToLoad in rasObjList:
            try:  # valid raster

                logging.debug('\t\tLoading raster: {0} as {1}'.format(rasterToLoad.origFile, rasterToLoad.loadFile))

                # At this point, we have built a raster load object that we can use. We still need to:
                #  I.) extract orig raster and save to the desired final mosaic folder as the load raster name.
                #      (This will be overwriting an existing file in the folder.)
                #  II.) load the 'load' raster to the proper mosaic dataset (again overwriting previously named rasters)
                #  III.) delete the original raster from the temp extract folder
                #  IV.) populate the loaded raster's attributes

                # Save the file to the final source folder and load it into the mosaic dataset
                # extract = arcpy.sa.ExtractByAttributes(raster, inSQLClause)
                extract = arcpy.sa.ExtractByAttributes(rasterToLoad.origFile, inSQLClause)
                loadRaster = os.path.join(final_RasterSourceFolder, rasterToLoad.loadFile)
                extract.save(loadRaster)
                # ----------
                #  For some reason, the extract is causing the raster attribute table (.tif.vat.dbf file) to be created
                # which is being locked (with a ...tif.vat.dbf.lock file) as users access the WMS service. The problem
                # is that the lock file is never released and future updates to the raster are failing. Therefore, here
                # we will just try to delete the raster attribute table right after it is created.
                arcpy.DeleteRasterAttributeTable_management(loadRaster)
                # ----------
                arcpy.AddRastersToMosaicDataset_management(rasterToLoad.targetDataset, "Raster Dataset", loadRaster,
                                                           "UPDATE_CELL_SIZES", "NO_BOUNDARY", "NO_OVERVIEWS",
                                                           "2", "#", "#", "#", "#", "NO_SUBFOLDERS",
                                                           "OVERWRITE_DUPLICATES", "BUILD_PYRAMIDS",
                                                           "CALCULATE_STATISTICS", "NO_THUMBNAILS",
                                                           "Add Raster Datasets", "#")
                # arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=rasterToLoad.targetDataset,
                #                                            raster_type="Raster Dataset",
                #                                            input_path=loadRaster,
                #                                            update_cellsize_ranges="UPDATE_CELL_SIZES",
                #                                            update_boundary="UPDATE_BOUNDARY",
                #                                            update_overviews="NO_OVERVIEWS",
                #                                            maximum_pyramid_levels="",
                #                                            maximum_cell_size="0",
                #                                            minimum_dimension="1500",
                #                                            spatial_reference="",
                #                                            filter="#",
                #                                            sub_folder="SUBFOLDERS",
                #                                            duplicate_items_action="ALLOW_DUPLICATES",
                #                                            build_pyramids="NO_PYRAMIDS",
                #                                            calculate_statistics="NO_STATISTICS",
                #                                            build_thumbnails="NO_THUMBNAILS",
                #                                            operation_description="#",
                #                                            force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE",
                #                                            estimate_statistics="NO_STATISTICS",
                #                                            aux_inputs="")

                # If we get here, we have successfully added the raster to the mosaic and saved it to its final
                # source location, so lets go ahead and remove it from the temp extract folder now...
                arcpy.Delete_management(rasterToLoad.origFile)

                try:  # Set Attributes
                    # Update the attributes on the raster that was just added to the mosaic dataset

                    # Initialize and build attribute expression list
                    attrExprList = [rasterToLoad.startDate, rasterToLoad.endDate]

                    # Get the raster name minus the .tif extension
                    rasterName_minusExt = os.path.splitext(rasterToLoad.loadFile)[0]
                    wClause = "Name = '" + rasterName_minusExt + "'"

                    with arcpy.da.UpdateCursor(rasterToLoad.targetDataset, attrNameList, wClause) as cursor:
                        for row in cursor:
                            for idx in range(len(attrNameList)):
                                row[idx] = attrExprList[idx]
                            cursor.updateRow(row)

                    del cursor

                except:  # Set Attributes
                    err = capture_exception()
                    logging.warning("\t...Raster attributes not set for raster {0}. Error = {1}".format(
                        rasterToLoad.origFile, err))

            except:  # valid raster
                err = capture_exception()
                logging.warning('\t...Raster {0} not loaded into mosaic! Error = {1}'.format(
                    rasterToLoad.origFile, err))

        del rasObjList[:]

    except:
        err = capture_exception()
        logging.error(err)


def refreshService(clsSvc):
    """
        Restart the ArcGIS Service (Stop and Start) using the URL token service and class object passed in.
    """

    # Try and stop the service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f": "json", "username": clsSvc.username,
                                        "password": clsSvc.password, "client": "requestip"})
        tokenResponse = urllib.urlopen(clsSvc.adminURL + "/generateToken?", tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the service
        stopParams = urllib.urlencode({"token": token, "f": "json"})
        stopResponse = urllib.urlopen(clsSvc.adminURL + "/services/" + clsSvc.folder + "/" + clsSvc.svcName + "." +
                                      clsSvc.svcType + "/stop?", stopParams).read()
        stopResponseJSON = json.loads(stopResponse)
        stopStatus = stopResponseJSON["status"]

        if "success" not in stopStatus:
            logging.warning("UNABLE TO STOP SERVICE " + clsSvc.folder + "/" + clsSvc.svcName +
                            "/" + clsSvc.svcType + " STATUS = " + stopStatus)
        else:
            logging.info("Service: " + clsSvc.svcName + " has been stopped.")

    except Exception, e:
        logging.error("### ERROR ### - Stop Service failed for " + clsSvc.svcName + ", System Error Message: " + str(e))

    # Try and start the service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f": "json", "username": clsSvc.username,
                                        "password": clsSvc.password, "client": "requestip"})
        tokenResponse = urllib.urlopen(clsSvc.adminURL + "/generateToken?", tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the current service
        startParams = urllib.urlencode({"token": token, "f": "json"})
        startResponse = urllib.urlopen(clsSvc.adminURL + "/services/" + clsSvc.folder + "/" + clsSvc.svcName + "." +
                                       clsSvc.svcType + "/start?", startParams).read()
        startResponseJSON = json.loads(startResponse)
        startStatus = startResponseJSON["status"]

        if "success" in startStatus:
            logging.info("Started service: " + clsSvc.folder + "/" + clsSvc.svcName + "/" + clsSvc.svcType)
        else:
            logging.warning("UNABLE TO START SERVICE " + clsSvc.folder + "/" + clsSvc.svcName +
                            "/" + clsSvc.svcType + " STATUS = " + startStatus)
    except Exception, e:
        logging.error("### ERROR ### - Start Service failed for " + clsSvc.svcName + ", System Error Message: " + str(e))


def main():
    try:

        # Setup any required and/or optional arguments to be passed in.
        args = setupArgs()

        # Check if the user passed in a log level argument, either DEBUG, INFO, or WARNING. Otherwise, default to INFO.
        if args.logging:
            log_level = args.logging
        else:
            log_level = "INFO"    # Available values are: DEBUG, INFO, WARNING, ERROR

        # Setup logfile
        logDir = GetConfigString("logFileDir")
        logPrefix = GetConfigString("logFilePrefix")
        logFilename = logPrefix + "_" + datetime.date.today().strftime('%Y-%m-%d') + '.log'
        FullLogFile = os.path.join(logDir, logFilename)
        logging.basicConfig(filename=FullLogFile,
                            level=log_level,
                            format='%(asctime)s: %(levelname)s --- %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info('====================================== SESSION START ===========================================')
        logging.info("\t\t\t" + getScriptName())

        # Get a start time for the entire script run process.
        time_TotalScriptRun = get_NewStart_Time()

        GDB_mosaic1Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("1DayDSName"))
        GDB_mosaic3Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("3DayDSName"))
        GDB_mosaic7Day = os.path.join(GetConfigString("GDBPath"), GetConfigString("7DayDSName"))
        DateTimeFormat = GetConfigString("GDB_DateFormat")

        # Get datetime for right now
        o_today_DateTime = datetime.datetime.strptime(datetime.datetime.now().strftime(DateTimeFormat), DateTimeFormat)

        # Create the Extract folder
        extractFolder = GetConfigString("extract_AccumulationsFolder")
        if not create_folder(extractFolder):
            logging.error("Could not create folder: {0}. Try to create manually and run again!".format(extractFolder))
            return

        logging.info("-----------------------------------------------------")
        logging.info("Getting Latest Accumulation Files from FTP (proxy)...")
        logging.info("-----------------------------------------------------")

        # Grab a timer reference
        time_ftpProcess = get_NewStart_Time()

        # Download the latest 1, 3, and 7 Day files from the FTP site into the Extract folder.
        logging.info("...using date {0} to determine source FTP folder.".format(o_today_DateTime.strftime('%m/%d/%Y %I:%M:%S %p')))
        # bGoodSoFar = ProcessAccumulationFiles(o_today_DateTime)
        bGoodSoFar = ProcessAccumulationFiles_FromProxy(o_today_DateTime)
        if not bGoodSoFar:
            logging.error("General Status: ProcessAccumulationFiles_FromProxy() returned an invalid status code.")
            return
        logging.info("\t=== PERFORMANCE ===>: ProcessAccumulationFiles_FromProxy took: " +
                     get_Elapsed_Time_As_String(time_ftpProcess))

        # At this point, the 1, 3, and 7 day raster files should be downloaded from the FTP site into the
        # extract folder and be ready to load into their respective mosaic dataset.
        logging.info("------------------------------------------------------------------")
        logging.info("Loading Accumulation rasters to their respective mosaic dataset...")
        logging.info("------------------------------------------------------------------")

        # Grab a timer reference
        time_loadProcess = get_NewStart_Time()

        # Load the 1, 3, and 7 Day files from the Extract folder to their respective mosaic dataset.
        LoadAccumulationRasters(extractFolder)
        logging.info("\t=== PERFORMANCE ===>: LoadingAccumulationFiles took: " +
                     get_Elapsed_Time_As_String(time_loadProcess))

        logging.info("-------------------------------------")
        logging.info("Performing geodatabase maintenance...")
        logging.info("-------------------------------------")

        # Grab a timer reference
        time_GDBMaintenanceProcess = get_NewStart_Time()

        # Do some routine maintenance on the GDB mosaic datasets...
        # No since in calculating statistics as this is now done as rasters are loaded into the mosaic dataset
        # logging.info("Calculating statistics...")
        # arcpy.CalculateStatistics_management(GDB_mosaic1Day, "1", "1", "#", "OVERWRITE", "#")
        # arcpy.CalculateStatistics_management(GDB_mosaic3Day, "1", "1", "#", "OVERWRITE", "#")
        # arcpy.CalculateStatistics_management(GDB_mosaic7Day, "1", "1", "#", "OVERWRITE", "#")
        logging.info("Compacting file geodatabase...")
        arcpy.Compact_management(GetConfigString("GDBPath"))
        logging.info("\t=== PERFORMANCE ===>: GDB Maintenance (Calc Stats and Compact) took: " +
                     get_Elapsed_Time_As_String(time_GDBMaintenanceProcess))

        logging.info("-----------------------------")
        logging.info("Refreshing the WMS service...")
        logging.info("-----------------------------")

        # Grab a timer reference
        time_RefreshServiceProcess = get_NewStart_Time()

        logging.info("Refreshing the services...")

        svc1Day = MapService()
        svc1Day.adminURL = GetConfigString('svc_adminURL')
        svc1Day.username = GetConfigString('svc_username')
        svc1Day.password = GetConfigString('svc_password')
        svc1Day.folder = GetConfigString('svc_folder')
        svc1Day.svcType = 'ImageServer'
        svc1Day.svcName = GetConfigString('svc_Name_1Day')

        svc3Day = MapService()
        svc3Day.adminURL = GetConfigString('svc_adminURL')
        svc3Day.username = GetConfigString('svc_username')
        svc3Day.password = GetConfigString('svc_password')
        svc3Day.folder = GetConfigString('svc_folder')
        svc3Day.svcType = 'ImageServer'
        svc3Day.svcName = GetConfigString('svc_Name_3Day')

        svc7Day = MapService()
        svc7Day.adminURL = GetConfigString('svc_adminURL')
        svc7Day.username = GetConfigString('svc_username')
        svc7Day.password = GetConfigString('svc_password')
        svc7Day.folder = GetConfigString('svc_folder')
        svc7Day.svcType = 'ImageServer'
        svc7Day.svcName = GetConfigString('svc_Name_7Day')

        svcAll = MapService()
        svcAll.adminURL = GetConfigString('svc_adminURL')
        svcAll.username = GetConfigString('svc_username')
        svcAll.password = GetConfigString('svc_password')
        svcAll.folder = GetConfigString('svc_folder')
        svcAll.svcType = 'MapServer'
        svcAll.svcName = GetConfigString('svc_Name_All')

        # Note the arcpy.PublishingTools.RefreshService() call must only be available at ArcGIS 10.6 and later
        # as it doesn't seem to work at 10.4
        ### arcpy.ImportToolbox(r'C:\temp\arcgis_localhost_siteadmin_USE_THIS_ONE.ags;System/Publishing Tools')
        ### arcpy.PublishingTools.RefreshService(svc1Day.svcName, svc1Day.svcType, svc1Day.folder, "#")
        ### arcpy.PublishingTools.RefreshService(svc3Day.svcName, svc3Day.svcType, svc3Day.folder, "#")
        ### arcpy.PublishingTools.RefreshService(svc7Day.svcName, svc7Day.svcType, svc7Day.folder, "#")
        ### arcpy.PublishingTools.RefreshService(svcAll.svcName, svcAll.svcType, svcAll.folder, "#")
        # ToDo... Enable these calls on the server...
        # refreshService(svc1Day)
        # refreshService(svc3Day)
        # refreshService(svc7Day)
        # refreshService(svcAll)
        # Update the JSON file used to verify service updates...
        jsonFile = GetConfigString('JSONFile_ServiceUpdates')
        UpdateServicesJsonFile(jsonFile,  svc1Day.svcName, o_today_DateTime)
        UpdateServicesJsonFile(jsonFile,  svc3Day.svcName, o_today_DateTime)
        UpdateServicesJsonFile(jsonFile,  svc7Day.svcName, o_today_DateTime)
        UpdateServicesJsonFile(jsonFile,  svcAll.svcName, o_today_DateTime)

        logging.info("\t=== PERFORMANCE ===>: RefreshServiceProcess took: " +
                     get_Elapsed_Time_As_String(time_RefreshServiceProcess))

        # Log the Grand total script execution time...
        logging.info("------------------------------------------------------------------------------------------------")
        logging.info("=== PERFORMANCE ===>: Grand Total Processing Time was: " +
                     get_Elapsed_Time_As_String(time_TotalScriptRun))

        logging.info("====================================== SESSION END =============================================")
        # Add a few lines so we can tell sessions apart in the log more quickly
        logging.info("")
        logging.info("")
        # END

    except:
        err = capture_exception()
        logging.error(err)


# Call Main Function
main()
