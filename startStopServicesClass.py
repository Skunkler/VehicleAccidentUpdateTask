import arcpy, httplib, urllib, json, sys
from arcpy import env


class StartStopServices():


    def __init__(self, username, password, serverPort, serverName):
        self._username = username
        self._password = password
        self._serverPort = serverPort
        self._serverName = serverName

       

    

    def __assertJsonSuccess(self, data):

        obj = json.loads(data)
        if "status" in obj and obj["status"] == "error":
            print "Error: JSON object returns an error. " + str(obj)
            return False
        else:
            return True


    def __getToken(self):
        tokenURL = "/arcgis/admin/generateToken"

        params = urllib.urlencode({'username': self._username, 'password': self._password, 'client': 'requestip', 'f':'json'})

        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept":"text/plain"}

        httpConn = httplib.HTTPConnection(self._serverName, self._serverPort)
        httpConn.request("POST", tokenURL, params, headers)

        response = httpConn.getresponse()

        if (response.status != 200):
            httpConn.close()
            print "Error while fetching tokens from admin URL. Please check the URL and try again."
            return
        else:
            data = response.read()
            httpConn.close()

            if not self.__assertJsonSuccess(data):
                return
            token = json.loads(data)
            return token['token']
        
    def restartService(self, folder, listOfServices=""):
         self._folder = folder
         token = self.__getToken()

         if token == "":
            print "Could not generate a token with the username and password provided."
            return
         folder = self._folder

         if str.upper(folder) == "ROOT":
            folder = ""
         else:
            folder += "/"

         folderURL = "/arcgis/admin/services/"+folder

         params = urllib.urlencode({"token":token, "f": "json"})

         headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}

         httpConn = httplib.HTTPConnection(self._serverName, self._serverPort)
         httpConn.request("POST", folderURL, params, headers)

         response = httpConn.getresponse()
         
         if (response.status != 200):
             httpConn.close()
             print "Could not read folder information."
             return
         else:
             data = response.read()
        
            # Check that data returned is not an error object
             if not self.__assertJsonSuccess(data):          
                 print "Error when reading folder information. " + str(data)
             else:
                 print "Processed folder information successfully. Now processing services..."

            # Deserialize response into Python object
             dataObj = json.loads(data)
             httpConn.close()
            
             for item in dataObj['services']:
                 if item['serviceName'] == listOfServices:
               
                     fullSvcName = item['serviceName'] + "." + item['type']
                     print(fullSvcName)
                     stopURL = "/arcgis/admin/services/" + folder + fullSvcName + "/STOP"
                     startURL="/arcgis/admin/services/" + folder + fullSvcName + "/START"

                     httpConn.request("POST", stopURL, params, headers)
                     stopResponse = httpConn.getresponse()
                     if (stopResponse.status != 200):
                         httpConn.close()
                         print "Error while executing stop or start. Please check the URL and try again."
                         return
                     else:
                         stopData = stopResponse.read()
                
            # Check that data returned is not an error object
                         if not self.__assertJsonSuccess(stopData):
                             if str.upper(stopData) == "START":
                                 print "Error returned when starting service " + fullSvcName + "."
                             else:
                                 print "Error returned when stopping service " + fullSvcName + "."

                                 print str(stopData)
                    
                         else:
                             print "Service " + fullSvcName + " processed successfully."

                     httpConn.close()

                     httpConn.request("POST", startURL, params, headers)
                     startResponse = httpConn.getresponse()
                     if(startResponse.status != 200):
                         httpConn.close()
                         print "Error while executing stop or start. Please check the URL and try again."
                         return
                     else:
                         startData = startResponse.read()
                         if not self.__assertJsonSuccess(startData):
                             if str.upper(stopData) == "START":
                                 print "Error returned when starting service " + fullSvcName + "."
                             else:
                                 print "Error returned when stopping service " + fullSvcName + "."

                                 print str(startData)
                    
                         else:
                             print "Service " + fullSvcName + " processed successfully."

                     httpConn.close()
                        
           
                    
    def openCloseConnection(self, folder, startOrStop, listOfServices=""):
        self._folder = folder
        token = self.__getToken()

        if token == "":
            print "Could not generate a token with the username and password provided."
            return
        folder = self._folder

        if str.upper(folder) == "ROOT":
            folder = ""
        else:
            folder += "/"

        folderURL = "/arcgis/admin/services/"+folder

        params = urllib.urlencode({"token":token, "f": "json"})

        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}

        httpConn = httplib.HTTPConnection(self._serverName, self._serverPort)
        httpConn.request("POST", folderURL, params, headers)

        response = httpConn.getresponse()
        if (response.status != 200):
            httpConn.close()
            print "Could not read folder information."
            return
        else:
            data = response.read()
        
            # Check that data returned is not an error object
            if not self.__assertJsonSuccess(data):          
                print "Error when reading folder information. " + str(data)
            else:
                print "Processed folder information successfully. Now processing services..."

            # Deserialize response into Python object
            dataObj = json.loads(data)
            httpConn.close()
            
            for item in dataObj['services']:
                if item['serviceName'] == listOfServices:
               
                    fullSvcName = item['serviceName'] + "." + item['type']
                    print(fullSvcName)
                # Construct URL to stop or start service, then make the request                
                    stopOrStartURL = "/arcgis/admin/services/" + folder + fullSvcName + "/" + startOrStop
                    httpConn.request("POST", stopOrStartURL, params, headers)

        # Read stop or start response
                    stopStartResponse = httpConn.getresponse()
                    if (stopStartResponse.status != 200):
                        httpConn.close()
                        print "Error while executing stop or start. Please check the URL and try again."
                        return
                    else:
                        stopStartData = stopStartResponse.read()
                
            # Check that data returned is not an error object
                        if not self.__assertJsonSuccess(stopStartData):
                            if str.upper(stopStartData) == "START":
                                print "Error returned when starting service " + fullSvcName + "."
                            else:
                                print "Error returned when stopping service " + fullSvcName + "."

                                print str(stopStartData)
                    
                        else:
                            print "Service " + fullSvcName + " processed successfully."

                    httpConn.close()
           




#startStop_SRV03 = StartStopServices("siteadmin", "admin", 6080, "caggissrv03")
#startStop_SRV03.openCloseConnection("GIS_Framework", "START",  "AA_Apps")
