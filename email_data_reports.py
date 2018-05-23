# Copyright 2018, Leanplum Inc.
# Author: Avery Tang (avery.tang@leanplum.com) Joe Ross (joseph.ross@leanplum.com)
# gcloud auth application-default login <-- provides env authentication to connect to bigquery/datastore

import gcloud
from google.cloud import datastore
import bigquery
from oauth2client.client import GoogleCredentials
import googleapiclient.discovery
import datetime
import argparse
import os
import re
import subprocess
import DomainLineQueryGen as DomainGenerator
import SubjectLineQueryGen as SubjectGenerator


#List of domains to breakout
Domains = "(\"gmail.com\",\"msn.com\",\"hotmail.com\", \"yahoo.com\")"

def retrieve_backup_files(service, date, bucket, newConvention):
    """retrieve all datastore backup file names for the date supplied

    :param service: object, googleapiclient
    :param date: str, data backup date in %Y%m%d format, e.g 20170425
    :param bucket, google storage bucket name
    :param newConvention: bool, data backup file convention name change
    :return: a list of file names

    Convention updates:
        After 2018-04-27: Stored in GCS at gs://leanplum_datastore_backups  (this is owned by the leanplum project)
            Folders are named in the following convention gs://leanplum_datastore_backups/20180427215301
        Before 2018-04-24: Stored in GCS at gs://leanplum_backups (this is owned by the leanplum2 project)
            Folders are named in the following convention gs://leanplum_backups/backup_201501062015_01_06_2015-01-06T10:00:03
    """
    search_str = ''

    if(newConvention):
        search_str = date
    else:
        search_str = "backup_" + date  # 20170313
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return, prefix=search_str, delimiter='output')

    files = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        files.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)

    filenames = [x['name'] for x in files]
  
    return filenames

def load_multi_table(service, client, dateStart, dateEnd, bucket, dataset, model):
    """import BQ table using datastore backups files over time range
    :param service: object, googleapiclient
    :param client: object, BigQuery client
    :param dateStart: str, YYYYMMDD
    :param dateEnd: str, YYYYMMDD
    :param bucket: str, google storage bucket name
    :param dataset: str, name for the data set where the table will be created
    :param model: str, datastore model
    :return: None
    """
    startDate = datetime.datetime.strptime(str(dateStart), '%Y%m%d')
    endDate = datetime.datetime.strptime(str(dateEnd), '%Y%m%d')
    date_generated = [startDate + datetime.timedelta(days=x) for x in range(0, (endDate - startDate + datetime.timedelta(days=1)).days)]

    for date in date_generated:
        try:
            load_table(service, client, date.strftime('%Y%m%d'), bucket, dataset, model)
            continue
        except bigquery.errors.JobInsertException:
            pass
            

def load_table(service, client, date, bucket, dataset, model):

    """import BQ table using datastore backup files
    :param service: object, googleapiclient
    :param client: object, BigQuery client
    :param date: str, YYYYMMDD
    :param bucket: str, google storage bucket name
    :param dataset: str, name for the data set where the table will be created
    :param model: str, datastore model
    :return: None
    """

    # logger.info("Retrieving backup files for {} on {}...".format(model, date))
    
    newConvention = False
    if( date > "20180426" ):
        bucket = "leanplum_datastore_backups"
        newConvention = True

    files = retrieve_backup_files(service, date, bucket, newConvention)
    model_search_str = ''
    if(newConvention):
        model_search_str = '_' + model.lower() + '.'
    else:
        model_search_str = "." + model.lower() + "."
    backup_file = [x for x in files if model_search_str in x.lower()]

    if len(backup_file) == 1:
        source_uri = 'gs://' + bucket + "/" + backup_file[0]
        table_name = model.title() + "_" + date

        if( not client.check_table(dataset=dataset, table=table_name) ):
            loading = client.import_data_from_uris(source_uris=source_uri,
                                                   dataset=dataset,
                                                   table=table_name,
                                                   source_format='DATASTORE_BACKUP')

            job_id = loading['jobReference']['jobId']
            print("Loading Model : " + model + "_backup - " + date, flush=True)
            job = client.wait_for_job(job_id, timeout=120)
            print("Model Loaded : " + model + "_backup - " + date, flush=True)
        #else:
            #print("Model : " + model + "_backup - " + date + " Exists", flush=True)

def create_default_sender_email_query(appId, endDate):
    query = """
    --GRAB FROM ADDRESS 
    SELECT 
        email_from_address 
    FROM 
        [leanplum-staging:email_report_backups.App_""" + endDate + """]  
    WHERE __key__.id = """ + appId
    return query

def create_sender_email_query(startDate, endDate):
    query = """
        ---Grab Sender emails
        SELECT
            study.id AS MessageID,
            vars.value.text AS SenderEmail,
        FROM
            (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
                TIMESTAMP('""" + startDate + """'),
                TIMESTAMP('""" + endDate + """')))
        WHERE (vars.name = "Sender email")
        """

    return query

def create_appids_query(companyId, endDate):
    appids = """
        --Grab ID's
        SELECT
            company.id as app_CompanyID,
            name as app_AppName,
            __key__.id as app_AppID
        FROM
            [leanplum-staging:email_report_backups.App_""" + endDate + """]
        WHERE STRING(company.id) = '""" + companyId + "\'"
    return appids

def runReport(companyId, startDate, endDate, reportType):

    # #Parse command line inputs
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--project', '-p', help='project id', default='leanplum-staging')
    # parser.add_argument('--dataset', '-d',  help='dataset that stores the tables', default="email_report_backups")
    # parser.add_argument('--model', '-m', help='datastore model(s) to load', nargs='+', default=['App','Study','Experiment'])
    # #We do not catch bad date format
    # parser.add_argument('--dateS', '-ts', help='start date YYYYMMDD', required=True)
    # parser.add_argument('--dateE', '-te', help='end date YYYYMMDD', required=True)
    # parser.add_argument('--bucket', '-b', help='google storage bucket name', default='leanplum_backups')
    # parser.add_argument('--company', '-c', help='company id', required=True)
    # parser.add_argument('--report', '-r', help='report type (s)ubject/(d)domain', required=True)
    # args = parser.parse_args()

    # initialize google and bq clients
    google_credential = GoogleCredentials.get_application_default()
    google_service = googleapiclient.discovery.build('storage', 'v1', credentials=google_credential)
    bq_client = bigquery.get_client(project_id='leanplum-staging', credentials=google_credential)

    #Load all the backups
    for model in ['App','Study','Experiment']:
        print("Loading " + model, flush=True)
        load_multi_table(service=google_service,
                   client=bq_client,
                   dateStart=startDate,
                   dateEnd=endDate,
                   bucket='leanplum_backups',
                   dataset='email_report_backups',
                   model=model)

    print("\tBackups Loaded")

    #Load Subject Report
    if(reportType == 's'):
        print('\tCreating report by Subject Line')
        appidsQuery = create_appids_query(companyId, endDate)

        #Create query for App Id's
        appJob = bq_client.query(appidsQuery)
        bq_client.wait_for_job(appJob[0],timeout=120)
        appResults = bq_client.get_query_rows(appJob[0])

        #Loop through all App Id's
        for appBundle in appResults:
            print("\n\tQuerying Data for " + appBundle['app_AppName'] + ":" + str(appBundle['app_AppID']))

            #In case the query fails because of missing data or a test app
            try:
                fileName = "EmailData_" + str(appBundle['app_AppName']) + "_" + str(startDate) + "_" + str(endDate) + "_subject.csv"
                file = open(fileName, "wb")
                file.write("Subject,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,MessageLink\n".encode('utf-8'))

                subjectLineQuery = create_subject_line_query(companyId, str(appBundle['app_AppID']), startDate, endDate)
                subjectLineJob = bq_client.query(subjectLineQuery)
                print("\t\tRunning Query", flush=True)
                bq_client.wait_for_job(subjectLineJob[0],timeout=120)
                print("\t\tQuery Success", flush=True)
                subjectResults = bq_client.get_query_rows(subjectLineJob[0])

                uniqLineQuery = create_subject_line_uniq_query(companyId, str(appBundle['app_AppID']), startDate, endDate)
                uniqLineJob = bq_client.query(uniqLineQuery)
                print("\t\tRunning Query for Uniques", flush=True)
                bq_client.wait_for_job(uniqLineJob[0],timeout=120)
                print("\t\tQuery Success", flush=True)
                uniqResults = bq_client.get_query_rows(uniqLineJob[0])

                #There is a difference between a bad table and a zero table. We catch that here.
                if(not subjectResults):
                    print("\t\tINFO: Zero Records Returned")
                    file.close()
                    os.remove(fileName)
                    continue

                #Loop through all the MessageId's that we gathered from the AppId
                for item in subjectResults:
                    for uni in uniqResults:
                        if(uni['MessageId'] == item['MessageId']):
                            if(int(item['Sent'] == 0)):
                                break

                            numString = ""

                            delivPct = 0.0
                            bouncePct = 0.0
                            openPct = 0.0
                            uniqueOpenPct = 0.0
                            uniqueClickPct = 0.0

                            if(float(item['Sent']) > 0.0):
                                delivPct = float(item['Delivered'])/float(item['Sent']) * 100.0
                                bouncePct = float(item['Bounce'])/float(item['Sent']) * 100.0
                            if(float(item['Delivered']) > 0.0):
                                openPct = float(item['Open'])/float(item['Delivered']) * 100.0
                                uniqueOpenPct = float(uni['Unique_Open'])/float(item['Delivered']) * 100.0
                                uniqueClickPct = float(uni['Unique_Click'])/float(item['Delivered']) * 100.0

                            numString += "\"" + item['Subject'] + "\","
                            #Removing MessageID as Excel malforms it.
                            #numString += str(item['MessageId']) + ","
                            numString += str(item['Sent']) + ","
                            numString += str(item['Delivered']) + ","
                            numString += str(delivPct)[:4] + "%,"
                            numString += str(item['Open']) + ","
                            numString += str(openPct)[:4] + "%,"
                            numString += str(uni['Unique_Open']) + ","
                            numString += str(uniqueOpenPct)[:4] + "%,"
                            numString += str(uni['Unique_Click']) + ","
                            numString += str(uniqueClickPct)[:4] + "%,"
                            numString += str(item['Bounce']) + ","
                            numString += str(bouncePct)[:4] + "%,"
                            numString += str(item['Dropped']) + ","
                            numString += str(item['Unsubscribe']) + ","
                            numString += "https://www.leanplum.com/dashboard?appId=" +  str(appBundle['app_AppID']) + "#/" + str(appBundle['app_AppID']) + "/messaging/" + str(item['MessageId']) + "\n"


                            file.write(numString.encode('utf-8'))
                            break
                file.close() 

                #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                lineCount = 0
                p = subprocess.Popen(['wc','-l',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                result, err = p.communicate()
                if p.returncode != 0:
                    print("\t\tINFO: Error reading end file")
                else:
                    lineCount = int(result.strip().split()[0])

                if(lineCount == 1):
                    print("\t\tINFO: Zero Records Returned. Deleting Report.")
                    os.remove(fileName)
                else:
                    print("\t\tSuccess")
                file.close()
            
            except googleapiclient.errors.HttpError as inst:
                print("\t\tWarning: This App had bad query. Deleting Report. " + str(type(inst)))
                file.close()
                os.remove(fileName)
                pass
        print("Finished Running Reports")
    #Domain Report
    elif(reportType == 'd'):
            print('\tCreating report by Domain against : ' + Domains)

            # attrFileName = "AppID_Attr.txt"

            #We use an attribute file to lookup email attr location from datastore.
            # try:
            #     attrFile = open(attrFileName, 'r+')
            #     print('\tAttribute File Found')
            # except:
            #     attrFile = open(attrFileName, 'w+')
            # attrLines = attrFile.readlines()
            # attrDict = {}
            # for line in attrLines:
            #     appid = re.search("[0-9]*",line).group(0)
            #     attrVal = re.search(":[0-9]*",line).group(0)[1:]
            #     attrDict[appid] = attrVal

            #Lookup App Id's for the company
            appidsQuery = create_appids_query(companyId, endDate)
            appJob = bq_client.query(appidsQuery)
            bq_client.wait_for_job(appJob[0],timeout=120)
            appResults = bq_client.get_query_rows(appJob[0])

            #Loop through all App's gathered
            for app in appResults:

                #In case the query fails because of missing data or a test app
                try:
                    print("\n\tQuerying Data for " + app['app_AppName'] + ":" + str(app['app_AppID']))
                    fileName = "EmailData_" + str(app['app_AppName']) + "_" + str(startDate) + "_" + str(endDate) + "_domain.csv"
                    file = open(fileName, "wb")
                    file.write("MessageName,SenderDomain,Domain,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,Type,MessageLink\n".encode('utf-8'))

                    attrLoc = ''

                    #Check if our attribute location was previously found - if not request lookup and append to file
                    # if str(app['app_AppID']) in attrDict:
                    #     attrLoc = attrDict[str(app['app_AppID'])]
                    # else:
                    #     attrLoc = input("Please enter location of email attribute for " + str(app['app_AppName'] + ": " + str(app['app_AppID']) + " :"))
                    #     attrFile.write(str(app['app_AppID']) + " :" + str(attrLoc) + "\n")

                    #Look up email attr in datastore
                    appId = int(app['app_AppID'])
                    #Create datastore entity
                    ds_client = datastore.Client(project='leanplum')
                    query = ds_client.query(kind='App')
                    key = ds_client.key('App',appId)
                    query.key_filter(key,'=')

                    emailName = ''
                    emailLoc = 0

                    #Do Query on Datastore
                    print("\t\tTapping Datstore:App", flush=True)
                    appList = list(query.fetch())

                    try:
                    #Should only return the AppData for appId specific in key
                        if(len(appList)!= 1):
                            print('\t\tBad App Entities returned from AppID for ' + str(app['app_AppName']) + '.Ignore for Unwanted Apps')
                        else:
                            emailName = dict(appList[0])['email_user_attribute']
                            #Run query against app data to find location of email attr
                            query = ds_client.query(kind='AppData')
                            key = ds_client.key('AppData',appId)
                            query.key_filter(key,'=')

                            print("\t\tTapping Datastore:AppData", flush=True)
                            appDataList = list(query.fetch())
                            if(len(appDataList) != 1):
                                print('\t\tBad AppData Entities returned from AppID for ' + str(app['app_AppName']) + '.Ignore for Unwanted Apps')
                            else:
                                #Count rows to find email location
                                attrColumns = dict(appDataList[0])['attribute_columns']
                                for attr in attrColumns:
                                    if(attr == emailName):
                                        break
                                    else:
                                        emailLoc = emailLoc + 1

                    except KeyError:
                        print("\t\tWarning: This App had bad datastore query.")
                        pass
                    #Set emailLocation to string - Lazy
                    attrLoc = str(emailLoc)
                    print('\t\tEmail Name : ' + emailName + ' : at Location : ' + attrLoc)

                    domainQuery = create_domain_line_query(str(app['app_AppID']), startDate, endDate, attrLoc)
                    domainJob = bq_client.query(domainQuery)
                    print("\t\tRunning Query for Domain", flush=True)
                    bq_client.wait_for_job(domainJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    domainResults = bq_client.get_query_rows(domainJob[0])

                    domainUniqueQuery = create_domain_unique_query(str(app['app_AppID']), startDate, endDate, attrLoc)
                    domainUniJob = bq_client.query(domainUniqueQuery)
                    print("\t\tRunning Query for Uniques", flush=True)
                    bq_client.wait_for_job(domainUniJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    domainUniResults = bq_client.get_query_rows(domainUniJob[0])

                    senderEmailQuery = create_sender_email_query(startDate, endDate)
                    senderJob = bq_client.query(senderEmailQuery)
                    print("\t\tRunning Query for Sender Emails", flush=True)
                    bq_client.wait_for_job(senderJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    senderEmailResults = bq_client.get_query_rows(senderJob[0])

                    defaultEmailSenderQuery = create_default_sender_email_query(str(app['app_AppID']), str(endDate))
                    defaultEmailJob = bq_client.query(defaultEmailSenderQuery)
                    print("\t\tRunning Query for Default Sender Email", flush=True)
                    bq_client.wait_for_job(defaultEmailJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    defaultEmail = bq_client.get_query_rows(defaultEmailJob[0])[0]['email_from_address']

                    #Used for All Category
                    allCategoryDict = {'MessageName':'','MessageID':0,'SenderDomain':'','Domain':'All','Sent':0,'Delivered':0,'Open':0,'Unique_Open':0,'Unique_Click':0,'Bounce':0,'Dropped':0,'Unsubscribe':0,'Type':'','MessageLink':''}

                    #Loop through all results and build report
                    for domainNum in domainResults:
                        for domainUni in domainUniResults:
                            if(str(domainNum['Domain']) == str(domainUni['Domain']) and str(domainNum['MessageID']) == str(domainUni['MessageID'])):
                                if(int(domainNum['Sent']) == 0 or int(domainNum['ID']) != 1):
                                    break
                                numString = ""
                                senderEmail = ""

                                #Look for the sender email
                                for senderDict in senderEmailResults:
                                    if str(senderDict['MessageID']) == str(domainNum['MessageID']):
                                        senderEmail = senderDict['SenderEmail']
                                if( len(senderEmail) == 0 ):
                                    senderEmail = defaultEmail
                                delivPct = 0.0
                                bouncePct = 0.0
                                openPct = 0.0
                                uniqueOpenPct = 0.0
                                uniqueClickPct = 0.0

                                if(float(domainNum['Sent']) > 0.0):
                                    delivPct = float(domainNum['Delivered'])/float(domainNum['Sent']) * 100.0
                                    bouncePct = float(domainNum['Bounce'])/float(domainNum['Sent']) * 100.0
                                if(float(domainNum['Delivered']) > 0.0):
                                    openPct = float(domainNum['Open'])/float(domainNum['Delivered']) * 100.0
                                    uniqueOpenPct = float(domainUni['Unique_Open'])/float(domainNum['Delivered']) * 100.0
                                    uniqueClickPct = float(domainUni['Unique_Click'])/float(domainNum['Delivered']) * 100.0

                                if(allCategoryDict['MessageID'] == 0):
                                    allCategoryDict['MessageID'] = domainNum['MessageID']
                                elif(allCategoryDict['MessageID'] != domainNum['MessageID']):
                                    #Aggregate
                                    try:
                                        allStr = ''
                                        allStr += allCategoryDict['MessageName'] + ','
                                        allStr += str(allCategoryDict['SenderDomain']) + ','
                                        allStr += str(allCategoryDict['Domain']) + ','
                                        allStr += str(allCategoryDict['Sent']) + ','
                                        allStr += str(allCategoryDict['Delivered']) + ','
                                        allStr += str(float(allCategoryDict['Delivered'])/float(allCategoryDict['Sent'])*100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Open']) + ','
                                        allStr += str(float(allCategoryDict['Open'])/float(allCategoryDict['Delivered']) * 100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Unique_Open']) + ','
                                        allStr += str(float(allCategoryDict['Unique_Open'])/float(allCategoryDict['Delivered']) * 100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Unique_Click']) + ','
                                        allStr += str(float(allCategoryDict['Unique_Click'])/float(allCategoryDict['Delivered']) * 100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Bounce']) + ','
                                        allStr += str(float(allCategoryDict['Bounce'])/float(allCategoryDict['Sent']) * 100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Dropped']) + ','
                                        allStr += str(allCategoryDict['Unsubscribe']) + ','
                                        allStr += str(allCategoryDict['Type']) + ','
                                        allStr += ' \n'

                                        #Don't Write If Nothing There
                                        if(allCategoryDict['Sent'] != 0):
                                            file.write(allStr.encode('utf-8'))
                                    except ZeroDivisionError:
                                        pass
                                    #Zero out and Update
                                    allCategoryDict = {'MessageName':'','MessageID':0,'SenderDomain':'','Domain':'All','Sent':0,'Delivered':0,'Open':0,'Unique_Open':0,'Unique_Click':0,'Bounce':0,'Dropped':0,'Unsubscribe':0,'Type':'','MessageLink':''}
                                    allCategoryDict['MessageID'] = domainNum['MessageID']

                                numString += "\"" + domainNum['MessageName'] + " (" + senderEmail +  ")\","
                                allCategoryDict['MessageName'] = "\"" + domainNum['MessageName'] + " (" + senderEmail +  ")\""

                                prefix = re.search(".*@",senderEmail).group(0)
                                domain = senderEmail[len(prefix):]
                                numString += str(domain) + ","
                                allCategoryDict['SenderDomain'] = str(domain)

                                #Removing Message ID as Excel Malforms
                                #numString += str(domainNum['MessageID']) + ","
                                numString += str(domainNum['Domain']) + ","

                                numString += str(domainNum['Sent']) + ","
                                allCategoryDict['Sent'] += domainNum['Sent']

                                numString += str(domainNum['Delivered']) + ","
                                allCategoryDict['Delivered'] += domainNum['Delivered']

                                numString += str(delivPct)[:4] + "%,"

                                numString += str(domainNum['Open']) + ","
                                allCategoryDict['Open'] += domainNum['Open']

                                numString += str(openPct)[:4] + "%,"

                                numString += str(domainUni['Unique_Open']) + ","
                                allCategoryDict['Unique_Open'] += domainUni['Unique_Open']

                                numString += str(uniqueOpenPct)[:4] + "%,"

                                numString += str(domainUni['Unique_Click']) + ","
                                allCategoryDict['Unique_Click'] += domainUni['Unique_Click']

                                numString += str(uniqueClickPct)[:4] + "%,"

                                numString += str(domainNum['Bounce']) + ","
                                allCategoryDict['Bounce'] += domainNum['Bounce']

                                numString += str(bouncePct)[:4] + "%," 

                                numString += str(domainNum['Dropped']) + ","
                                allCategoryDict['Dropped'] += domainNum['Dropped']

                                numString += str(domainNum['Unsubscribe']) + ","
                                allCategoryDict['Unsubscribe'] += domainNum['Unsubscribe']

                                numString += str(domainNum['Type']) + ","
                                allCategoryDict['Type'] = str(domainNum['Type'])

                                numString += "https://www.leanplum.com/dashboard?appId=" +  str(app['app_AppID']) + "#/" + str(app['app_AppID']) + "/messaging/" + str(domainNum['MessageID']) + "\n"

                                file.write(numString.encode('utf-8'))
                                break

                    file.close()
                    #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                    lineCount = 0
                    p = subprocess.Popen(['wc','-l',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    result, err = p.communicate()
                    if p.returncode != 0:
                        print("\t\tINFO: Error reading end file")
                    else:
                        lineCount = int(result.strip().split()[0])

                    if(lineCount == 1):
                        print("\t\tINFO: Zero Records Returned. Deleting Report")
                        os.remove(fileName)
                    else:
                        print("\t\tSuccess")
                    file.close()
                except googleapiclient.errors.HttpError as inst:
                    print("\t\tWarning: This App had bad query. Deleting Report. " + str(type(inst)))
                    file.close()
                    os.remove(fileName)
                    pass
            #attrFile.close()
            print("Finished Running Reports")