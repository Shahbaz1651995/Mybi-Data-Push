id#!/usr/bin/env python
import json
import requests
import re
from bs4 import BeautifulSoup
from pandas import DataFrame
import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import lxml
import datetime
from datetime import datetime
now = datetime.now()

#os.chdir("C:\\Users\\av076028\\OneDrive - Cerner Corporation\\Alya\\Script")

now = datetime.now()
time=now.strftime("%Y-%m-%d %H:%M")
#import pymysql
path=os.getcwd()
path="C:\\Users\\MS076027\\OneDrive - Cerner Corporation\\Documents\\Vishnu\\Bala\\Updated SSE"
LOG_FILENAME = 'log_mybi.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

logging.debug('This message should go to the log file')
logging.debug(path)


def mybi_login(username, password):
    # Login Request String
    login = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v6="urn://oracle.bi.webservices/v6">
                    <soapenv:Body>
                        <v6:logon>
                            <v6:name>""" + username + """</v6:name>
                            <v6:password>""" + password + """</v6:password>
                        </v6:logon>
                    </soapenv:Body>
                </soapenv:Envelope>"""

    # Request Options
    options = {'Content-Type': 'application/xml'}
    # Web Service URL

    url = "https://mybi.cerner.com/analytics-ws/saw.dll?SoapImpl=nQSessionService"

    try:
        # Response Call
        response = requests.post(url=url, headers=options, data=login)
        # Extract Session Id
        session_id = re.search('<sawsoap:sessionID xsi:type="xsd:string">(.*)<\/sawsoap:sessionID>',
                               str(response.content)).group(1)

        if response.status_code is 200:
            # Print Notification
            print("Login successful")
        return session_id

    except Exception as e:
        # Print Error
        print("\nError: " + e)


def get_report(report_path, session_id):
    report_call = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:v6="urn://oracle.bi.webservices/v6">
                        <soapenv:Header/>
                        <soapenv:Body>
                            <v6:executeXMLQuery>
                                <v6:report>
                                    <v6:reportPath>""" + report_path + """</v6:reportPath>
                                </v6:report>
                                <v6:outputFormat>SAWRowsetSchemaAndData</v6:outputFormat>
                                <v6:executionOptions>
                                     <v6:async></v6:async>
                                     <v6:refresh>false</v6:refresh>
                                    <v6:maxRowsPerPage></v6:maxRowsPerPage>
                                </v6:executionOptions>
                                <v6:sessionID>""" + session_id + """</v6:sessionID>
                            </v6:executeXMLQuery>
                        </soapenv:Body>
                    </soapenv:Envelope>"""

    # Request Options
    options = {'Content-Type': 'application/xml'}

    # Web Service URL
    url = "https://mybi.cerner.com/analytics-ws/saw.dll?SoapImpl=xmlViewService"
    try:
        response = requests.post(url=url, headers=options, data=report_call)
        report = response.text

        if report is not '':
            print("Report returned sucessfully")
        return report

    except Exception as e:
        # Print Error
        print('\nError: ', e)
        return e


def mybi_logout(session_id):
    # Logout Request String
    logout = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v6="urn://oracle.bi.webservices/v6">
                    <soapenv:Body>
                        <v6:logoff>
                            <v6:sessionID>""" + session_id + """</v6:sessionID>
                        </v6:logoff>
                    </soapenv:Body>
                </soapenv:Envelope>"""

    # Response Options
    options = {'Content-Type': 'application/xml'}
    # Response URL
    url = "https://mybi.cerner.com/analytics-ws/saw.dll?SoapImpl=nQSessionService"
    try:
        # Response Call
        response = requests.post(url=url, headers=options, data=logout)
        # Print Message
        if response.status_code is 200:
            print("Logoff successful")

    except Exception as e:
        # Print Error
        print("\nError: " + e)


def writexml(report, xmlfile):
    #report=retrieved_report
    #xmlfile=report_xml
    report = report.replace("&lt;", "<")
    report = report.replace("&gt;", ">")
    report = report.encode('ascii', 'ignore').decode('ascii')
    file_pointer_xml = open(xmlfile, 'w')
    file_pointer_xml.write(report)

#writetodatabase_sol(report_xml, db_username, db_password, db_server_name, db_name)

def writetodatabase_sol(report_xml, db_username, db_password, db_server_name, db_name,
                        db_table):
    assigned_xml_data = open(report_xml, 'r')
    #unassigned_xml_data = open(sol_unassigned_xml, 'r')

    assigned_xmlwriter = BeautifulSoup(assigned_xml_data, 'xml')
    #unassigned_xmlwriter = BeautifulSoup(unassigned_xml_data, 'xml')

    col0, col1, col2,col3,col4,col5,col6,col7,col8,col9,col10,col11, col12,col13,col14 = [],[],[], [], [], [], [], [], [], [], [],[],[],[], []

    for row in assigned_xmlwriter.find_all('Row'):
        col0.append(row.Column0.text)  # Change ID # #
        col1.append(row.Column1.text)  # Status
        col2.append(row.Column2.text)  # Summary
        col3.append(row.Column3.text)  # Template Name
        col4.append(row.Column4.text)  # Coordinator Support Group
        col5.append(row.Column5.text)  # Associate 
        col6.append(row.Column6.text)  #Manager
        col7.append(row.Column7.text)  #TAT
        col8.append(row.Column8.text)
        col9.append(row.Column9.text)
        col10.append(row.Column10.text)
        col11.append(row.Column11.text)  # Change ID # #
        col12.append(row.Column12.text)  # Status
        col13.append(row.Column13.text)  # Summary
        col14.append(row.Column14.text)  # Template Name
        #Manager#OLA
          #OLA Days
          # Summary
        
        


    
    df_unassigned = pd.DataFrame({'Request Type':col0,
                                'Request':col1,
                                'Status':col2,
                                'Impact':col3,
                                'OLA Met %':col4,
                                'Description':col5,
                                'Completed Date':col6,
                                'Request SLA':col7,
                                'Client':col8,
                                'Technology':col9,
                                'Associate':col10,
                                'Region':col11,
                                'Escalation':col12,
                                'Summary':col13,
                                'OLA VS No OLA':col14})
    
    import datetime as dt
    #df_unassigned['CompletedDateTime'] = df_unassigned['CompletedDateTime'].apply(lambda x : pd.to_datetime(str(x)))
    #df_unassigned['CompletedTime'] = [datetime.time(d) for d in df_unassigned['CompletedDateTime']]
    df_unassigned['Completed Date'] = pd.to_datetime(df_unassigned['Completed Date'])
    #df_unassigned['Today'] = now
    #df_unassigned1 = df_unassigned[df_unassigned.Technology == 'Backend']
    #df_unassigned2 = df_unassigned[df_unassigned.Technology == 'Frontend']
    #df_unassigned2_1 = df_unassigned2[:5000]
    #df_unassigned2_2 = df_unassigned2[5000:]
    #df_unassigned3 = df_unassigned[df_unassigned.Technology == 'Network']
    #df_unassigned3_1 = df_unassigned3[:5000]
    #df_unassigned3_2 = df_unassigned3[5000:]
    #df_unassigned4 = df_unassigned[df_unassigned.Technology == 'Storage']
    #df_unassigned5 = df_unassigned[df_unassigned.Technology == 'Provision']
    
    
    
    engine = create_engine('mysql+pymysql://' + db_username + ':' + db_password + '@' + db_server_name + '/' + db_name, echo=False)
    #engine=  create_engine("mysql+pymysql://cts1:Cerner123!@ctsanalyticsprod:3306/SD_CM?charset=utf8")
    conn = engine.connect()
    cnx = engine.raw_connection()

    olddate = (conn.execute("DROP table `Closed_Requests`"))
    #dt = olddate[0][0]
    #tm = pd.to_datetime(str(olddate[0][0])).dt.time
    #df_unassignedfinal = df_unassigned[df_unassigned['CompletedDateTime'] > dt]
    
    #df_unassigned.to_sql(name=db_table, con=engine, if_exists='append', index=False)
    
    ###initial for dumping value to db
    df_unassigned.to_sql(name=db_table, con=engine, if_exists='replace', index=False)
   
    
    #if engine.has_table(db_table):
    #df_unassignedfinal.to_sql(name=db_table, con=engine, if_exists='append', index=False)
    print("Report successfully added to the table - {} ".format(db_table))
    #else:
        #print("{} table does not exist in the database".format(db_table))
    cnx.close()
    conn.close()
    engine.dispose()
    
    
    

"""
username="svc_AUTO_CTS"
password="Cerner123"
ReportPath="/shared/system/SOAP_AUTO_CTS/Change Management Backend"
db_username="ctsuser"
db_password="Ctsanalytics1!"
db_server_name="ctsanalyticsprod"
db_name= "PowerBI_Backend"
db_table= "Change Management Backend"
	
"""




def main():
    
    with open(path + '/closed_request.json') as data_file:
        data = json.load(data_file)
    
    username = data["MyBI_Username"]
    password = data["MyBI_Password"]
    ReportPath = data["ReportPath"]
    session_id = mybi_login(username, password)
    
    # get the report in xml format
    retrieved_report = get_report(ReportPath, session_id)
    report_xml = './Scorecard_Malvern_task_CR1%s.xml'
    writexml(retrieved_report, report_xml)
    
    
    #Start connection to Database for update
  
    db_username = data["DBUsername"]
    db_password = data["DBPassword"]
    db_server_name = data["DBServerName"]
    db_name = data["DBName"]
    db_table = data["DBTableCRClosed"]
    
    
    logging.debug(data)
    
    writetodatabase_sol(report_xml, db_username, db_password, db_server_name, db_name, db_table)
            


if __name__ == '__main__':
    main()
