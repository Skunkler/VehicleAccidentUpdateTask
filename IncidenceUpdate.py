#Written By Warren Kunkler, posted on 09/28/2021, this is the automated task behind updated the car accident of the ECC data service every
#30 Minutes for live interactive mapping

import arcpy
from arcpy import env
from startStopServicesClass import StartStopServices

userName = ""
passWord = ""

startStop_SRV03 = StartStopServices(userName, passWord, 6080, "caggissrv03")


#code block used in field calculator to correct format of lat lon coordinates from string to floating point number
codeBlock = """def corLong(inputVal, needNeg=False):
  if int(inputVal) == 0:
    return 0
  elif needNeg == True:
    return ( int(inputVal) / 1000000.0)*-1
  else:
    return int(inputVal)/1000000.0"""



#function that takes mxd location and finds the connection within the mxd and the table
#then locates the output location within the web adapter and calls the defineLayer function using the table, output gdb, table output, and mxd location
def getTableView(mxdLoc):
    mxd = arcpy.mapping.MapDocument(mxdLoc)
    table = filter(lambda table: table.dataSource == 'Database Connections\Connection to ECC-DATA.sde\Reporting_System."COCAD\WKUNKLER".%Response_Master_Incident',arcpy.mapping.ListTableViews(mxd))
    outTable="Response_incident_past_30"
    outGDB = r"\\caggiswa01\GISFileDS\arcgisserver\Data\ResponseIncident.gdb"
    defineLayer(table[0],outGDB,outTable, mxd)


#function used for processing data to make the final output feature class used to define the output feature layer
def defineLayer(sdeInput, outputGDB, outTableName,mxd):
    print("starting process")
    env.workspace = outputGDB
    env.overwriteOutput = True
    arcpy.TruncateTable_management(outputGDB + '\\' + outTableName)

    #Time_PhonePickUp >= DATEADD(minute, -30, GETDATE()) AND Call_Is_Active = 1 AND Problem IN ('ACC','ACCBDG-COMBINED', 'ACCP-PD TRAP', 'FIREPC-COLAPS', 'FIREPS-STRUCT','FIREP-STILL','HAZARD','TRK', 'VEH') 
    arcpy.TableToTable_conversion(sdeInput, out_path=outputGDB, out_name=outTableName, where_clause="Time_PhonePickUp >= DATEADD(minute, -30, GETDATE()) AND Call_Is_Active = 1 AND Problem IN ('ACC','ACCBDG-COMBINED', 'ACCP-PD TRAP', 'FIREPC-COLAPS', 'FIREPS-STRUCT','FIREP-STILL','HAZARD','TRK', 'VEH')")
    print("Now defining fields")
    processTable=outputGDB + '\\'+outTableName
    arcpy.AddField_management(processTable, "longCor", "DOUBLE")
    arcpy.AddField_management(processTable, "latCor", "DOUBLE")
    arcpy.AddField_management(processTable, "TimeReported", "TEXT")
    arcpy.CalculateField_management(processTable, "longCor", "corLong( !Longitude!,True )", "PYTHON_9.3", codeBlock)
    arcpy.CalculateField_management(processTable, "latCor","corLong( !Latitude!)", "PYTHON_9.3", codeBlock)

    arcpy.CalculateField_management(processTable, "TimeReported", "str(!Time_PhonePickUp!)", "PYTHON_9.3", "")
    arcpy.MakeXYEventLayer_management(outTableName, in_x_field="longCor", in_y_field="latCor", out_layer="Response_incident_past_30_daysEvent", spatial_reference="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;0.001;0.001;IsHighPrecision", in_z_field="")
    if arcpy.Exists(outputGDB + "\\Response_incident_past_30_days"):
        print("deleting previous version of Response Incident_past 30 days")
        arcpy.Delete_management(outputGDB+"\\Response_incident_past_30_days")
    
    arcpy.FeatureClassToFeatureClass_conversion("Response_incident_past_30_daysEvent", outputGDB, "Response_incident_past_30_days")

    fieldNameList = ["OBJECTID", "Shape", "Master_Incident_Number", "TimeReported", "Agency_Type", "Jurisdiction", "Problem", "Location_Name", "Address", "City", "State", "Postal_Code", "County", "latCor", "longCor"]
    arcpy.MakeFeatureLayer_management(outputGDB+"\\Response_incident_past_30_days", "lyr")
    
    listOfDropFields = [field.name for field in list(filter(lambda field: field.name not in fieldNameList,arcpy.ListFields("lyr")))]   
    
    arcpy.DeleteField_management("lyr", listOfDropFields)

    mxd.save()
    print("Process complete!")

startStop_SRV03 = StartStopServices(userName, passWord, 6080, "caggissrv03")
Services = {"PROJECTS": "ResponseIncident"}


#starts task, calls the table function then the startStop services class to restart the map service when the process is complete
try:
    
    inMxd=r"\\caggissrv03\c$\ArcMapProjectsforArcServer\PROJECTS\ResponseIncident.mxd"
    getTableView(inMxd)
    startStop_SRV03.restartService("PROJECTS", Services["PROJECTS"])
    
    
except Exception as e:
    print(e)
    startStop_SRV03.openCloseConnection("PROJECTS", "START", Services["PROJECTS"])
    
