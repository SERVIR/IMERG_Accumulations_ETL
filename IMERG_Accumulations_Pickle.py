import pickle

mydict = {'extract_AccumulationsFolder': 'E:\ETLScratch\IMERG_Extract\Accumulations',
          'final_Folder': 'E:\SERVIR\Data\Global\IMERG_Accumulations',
          'logFileDir': 'E:\Code\IMERG_Accumulations_ETL\Log',
          'logFilePrefix': 'IMERG_Accumulations',
          'GDBPath': 'E:/SERVIR/DATA/Global/IMERG_Accumulations_SR3857.gdb',
          '1DayDSName': 'IMERG1Day',
          '3DayDSName': 'IMERG3Day',
          '7DayDSName': 'IMERG7Day',
          'rasterStartTimeProperty': 'start_datetime',
          'rasterEndTimeProperty': 'end_datetime',
          'RegEx_StartDateFilterString': '\d{4}[01]\d[0-3]\d-S[0-2]\d{5}',
          'GDB_DateFormat': '%Y%m%d%H%M',
          'Filename_StartDateFormat': '%Y%m%d-S%H%M%S',
          'ftp_host': 'jsimpson.pps.eosdis.nasa.gov',
          'ftp_user': 'SOMEVALUE',
          'ftp_pswrd': 'SOMEVALUE',
          'ftp_baseLateFolder': '/data/imerg/gis',
          'svc_adminURL': 'https://gis1.servirglobal.net/arcgis/admin',
          'svc_username': 'SOMEVALUE',
          'svc_password': 'SOMEVALUE',
          'svc_folder': 'Test',
          'svc_Name_1Day': 'IMERG1Day_ImgSvc',
          'svc_Name_3Day': 'IMERG3Day_ImgSvc',
          'svc_Name_7Day': 'IMERG7Day_ImgSvc',
          'svc_Type': 'ImageServer'}

output = open('config.pkl', 'wb')
pickle.dump(mydict, output)
output.close()
