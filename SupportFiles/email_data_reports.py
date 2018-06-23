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
from SupportFiles import DomainLineQueryGen as DomainGenerator
from SupportFiles import SubjectLineQueryGen as SubjectGenerator
from SupportFiles import PushNotificationQueryGen as PushGenerator


#List of domains to breakout
Domains = "(\"gmail.com\",\"msn.com\",\"hotmail.com\", \"yahoo.com\", \"aol.com\")"

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

def remove_multi_table(client, dateStart, dateEnd, dataset):
    startDate = datetime.datetime.strptime(str(dateStart), '%Y%m%d')
    endDate = datetime.datetime.strptime(str(dateEnd), '%Y%m%d')
    date_generated = [startDate + datetime.timedelta(days=x) for x in range(0, (endDate - startDate + datetime.timedelta(days=1)).days)]

    for date in date_generated:
        print("Deleting Tables for : " + date.strftime('%Y%m%d'),flush=True)
        try:
            remove_table(client,date.strftime('%Y%m%d'),dataset)
        except:
            print("Error Removing Tables",flush=True)
            return
            
def remove_table(client, date, dataset):
    for table_name in ["Study_","App_","Experiment_"]:
        table_name += date
        if( client.check_table(dataset=dataset, table=table_name) ):
            removing = client.delete_table(dataset=dataset,table=table_name)
            if(removing != True):
                print("Could not delete table :: " + removing,flush=True)

def load_message_ids(client, dataset, appId):

    appId = int(appId)

    ds_client = datastore.Client(project='leanplum')

    query = ds_client.query(kind='Study')
    key = ds_client.key('App',appId)
    query.add_filter('app','=',key)
    messageList = list(query.fetch())
    emailList = []
    for entity in messageList:
        if(entity['action_type'] == '__Email'):
            emailList += [entity.key.id]

    messageIdQuery = create_message_id_list_query(emailList)

    #Try and delete if previous exists
    delete_generic_table(client=client, table="Email_Message_Ids_" + str(appId),dataset=dataset)
    creation = client.create_table(
        dataset=dataset,
        table= "Email_Message_Ids_" + str(appId),
        schema={"name":"MessageId","type":"integer","mode":"nullable"}
        )
    if creation == True:
        # try:
        messageIdJob = client.write_to_table(
            query=messageIdQuery,
            dataset=dataset,
            table="Email_Message_Ids_" + str(appId),
            use_legacy_sql=False,
            )
        messageIdResults = client.wait_for_job(messageIdJob)
        # except:
        #     print("Error Saving to Table")
    else:
        print("Error Creating Table :: " + creation )

def delete_generic_table(client, table, dataset):
    if( client.check_table(dataset=dataset, table=table)):
        client.delete_table(dataset=dataset, table=table)

def create_message_id_list_query(messageIdList):
    query="""
--Unnest Literal Array
SELECT *
FROM UNNEST(""" + str(messageIdList) + """) as MessageId
"""
    return query
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
            study.id AS MessageId,
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
            company.id as CompanyId,
            name as AppName,
            __key__.id as AppId
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

    #Delete backups over time range
    if(reportType == 'delete'):
        print("Removing backups over timerange")
        remove_multi_table(
            client=bq_client,
            dateStart=startDate,
            dateEnd=endDate,
            dataset='email_report_backups'
            )
        print("\tBackups removed for time range")
        return

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
    if(reportType[0] == 's'):

        print('\tCreating report by Subject Line',flush=True)
        appidsQuery = create_appids_query(companyId, endDate)

        #Create query for App Id's
        appJob = bq_client.query(appidsQuery)
        bq_client.wait_for_job(appJob[0],timeout=120)
        appResults = bq_client.get_query_rows(appJob[0])

        #Loop through all App Id's
        for appBundle in appResults:
            print("\n\tRunning Report on App :: " + appBundle['AppName'] + ":" + str(appBundle['AppId']),flush=True)

            load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(appBundle['AppId']))

            ds_client = datastore.Client(project='leanplum')
            try:
                print("\t\tTapping Datastore for App Categories",flush=True)
                appQuery = ds_client.query(kind='App')
                appKey = ds_client.key('App',int(appBundle['AppId']))
                appQuery.key_filter(appKey,'=')
                appList = list(appQuery.fetch())
                categoryList = appList[0]['unsubscribe_categories']
            except KeyError:
                print("\t\tINFO: No UnsubscribeCategories",flush=True)
                categoryList = []
            except IndexError:
                print("\t\tWARNING: Could not find AppId in datastore",flush=True)
                categoryList = []
            #In case the query fails because of missing data or a test app
            try:
                fileName = "./Reports/EmailData_" + str(appBundle['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + "_subject.csv"
                directory = os.path.dirname(fileName)
                if not os.path.exists(directory):
                    os.makedirs(directory)
                file = open(fileName, "wb")
                file.write("Subject,StartDate,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,Spam,Spam_PCT,Category,MessageLink\n".encode('utf-8'))

                subjectLineQuery = SubjectGenerator.create_subject_line_query(startDate, endDate, str(appBundle['AppId']))
                print("\t\tRunning Query", flush=True)
                subjectLineJob = bq_client.query(subjectLineQuery)
                bq_client.wait_for_job(subjectLineJob[0],timeout=120)
                print("\t\tQuery Success", flush=True)
                subjectResults = bq_client.get_query_rows(subjectLineJob[0])

                uniqLineQuery = SubjectGenerator.create_unique_line_query(startDate, endDate, str(appBundle['AppId']))
                print("\t\tRunning Query for Uniques", flush=True)
                uniqLineJob = bq_client.query(uniqLineQuery)
                bq_client.wait_for_job(uniqLineJob[0],timeout=120)
                print("\t\tQuery Success", flush=True)
                uniqResults = bq_client.get_query_rows(uniqLineJob[0])

                #There is a difference between a bad table and a zero table. We catch that here.
                if(not subjectResults):
                    print("\t\tINFO: Zero Records Returned")
                    file.close()
                    os.remove(fileName)
                    continue

                #Check if we are running AB Reports before we spend the cash money
                if( reportType[1] == "1" ):
                    print("\t\t----AB Query On----",flush=True)
                    abQuery = SubjectGenerator.create_ab_query(startDate, endDate, str(appBundle['AppId']))
                    print("\t\tRunning AB Query", flush=True)
                    abJob = bq_client.query(abQuery)
                    bq_client.wait_for_job(abJob[0],timeout=240)
                    print("\t\tQuery Success", flush=True)
                    abResults = bq_client.get_query_rows(abJob[0])
                    print("\t\t\t" + str(len(abResults)) + " Variants Found",flush=True)

                    abUniqueQuery = SubjectGenerator.create_unique_ab_query(startDate, endDate, str(appBundle['AppId']))
                    print("\t\tRunning AB Unique Query", flush=True)
                    abUniqueJob = bq_client.query(abUniqueQuery)
                    bq_client.wait_for_job(abUniqueJob[0],timeout=240)
                    print("\t\tQuery Success", flush=True)
                    abUniqueResults = bq_client.get_query_rows(abUniqueJob[0])
                    print("\t\t\t" + str(len(abUniqueResults)) + " Unique Variants Found",flush=True)

                    variantSLQuery = SubjectGenerator.variant_subject_line_query(startDate, endDate, str(appBundle['AppId']))
                    print("\t\tRunning AB Subject Line Query", flush=True)
                    variantSLJob = bq_client.query(variantSLQuery)
                    bq_client.wait_for_job(variantSLJob[0],timeout=120)
                    print("\t\tQuery Success",flush=True)
                    variantSLResults = bq_client.get_query_rows(variantSLJob[0])
                    print("\t\t\t" + str(len(variantSLResults)) + " Variant Subject Lines Found", flush=True)

                #Loop through all the MessageId's that we gathered from the AppId
                for item in subjectResults:
                    for uni in uniqResults:
                        if(uni['MessageId'] == item['MessageId']):
                            if(int(item['Sent'] == 0)):
                                print("\t\tINFO: Skipping MessageId :: " + str(item['MessageId']) + " :: due to no `Sent` events for time range",flush=True)
                                break

                            #print(abResults,flush=True)
                            #print(abUniqueResults,flush=True)

                            #Grab messageId startDate
                            messageStartDate = ""
                            categoryId = -1
                            categoryName = "Default"
                            query = ds_client.query(kind='Study')
                            key = ds_client.key('Study',int(item['MessageId']))
                            query.key_filter(key,'=')
                            qList = list(query.fetch())
                            ## INFO 
                                # [0] :: Get the payload from the query (There's only one)
                                # ['active_since'] :: Payload is a dictionary
                                # .dat() :: In this case a datetime object is returned
                            try:
                                if( str(qList[0]['action_type']) != "__Email" ):
                                    print("\t\tWarning: Captured wrong message :: " + str(item['MessageId']) + " :: Type = " + str(qList[0]['action_type']), flush=True)
                                    break
                                messageStartDate = str(qList[0]['active_since'].date())
                                try:
                                    categoryId = int(qList[0]['category_id'])
                                    for categoryDict in categoryList:
                                        if(categoryId == int(categoryDict['id'])):
                                            categoryName = str(categoryDict['name'])
                                            break
                                except TypeError:
                                    #message doesn't have category
                                    pass
                            except IndexError:
                                pass
                            except AttributeError:
                                pass

                            if(messageStartDate == ""):
                                print("\t\t\tDataStore has no record of MessageId STUDY:: " + str(item['MessageId']),flush=True )
                                messageStartDate = "Unknown"

                            #Check if this messageId is apart of an AB Test
                            inExperiment = False
                            abDataRows = []

                            #Check if we are running AB reports
                            if( reportType[1] == "1" ):
                                for abInitialData in abResults:
                                    if( str(item['MessageId']) == str(abInitialData['MessageId'])):
                                        abDataRows += [abInitialData]
                                        inExperiment = True

                            numString = ""

                            if(inExperiment):
                                abUniqueDataRows = []

                                #Grab Unique Rows now that we know we have AB data
                                for abUniqueData in abUniqueResults:
                                    if item['MessageId'] == abUniqueData['MessageId']:
                                        abUniqueDataRows += [abUniqueData]

                                counter = 1
                                #Loop through variants
                                for abData in abDataRows:
                                    #print("Running Variant : " + str(counter) + " = " + str(abData['ExperimentVariant']),flush=True)
                                    counter += 1

                                    delivPct = 0.0
                                    bouncePct = 0.0
                                    openPct = 0.0
                                    uniqueOpenPct = 0.0
                                    uniqueClickPct = 0.0
                                    spamPct = 0.0

                                    uniAb = {}
                                    for abUniqueData in abUniqueDataRows:
                                        if( (abData['MessageId'] == abUniqueData['MessageId']) and (abData['ExperimentVariant'] == abUniqueData['ExperimentVariant']) ):
                                            uniAb = abUniqueData
                                            break

                                    variantSL = str(item['Subject'])
                                    for variantSubjectLines in variantSLResults:
                                        if( (abData['MessageId'] == variantSubjectLines['MessageId']) and (abData['ExperimentVariant'] == variantSubjectLines['ExperimentVariant']) ):
                                            variantSL = variantSubjectLines['SubjectLine']
                                            break

                                    if(float(abData['Sent']) > 0.0):
                                        delivPct = float(abData['Delivered'])/float(abData['Sent']) * 100.0
                                        bouncePct = float(abData['Bounce'])/float(abData['Sent']) * 100.0
                                    if(float(abData['Delivered']) > 0.0):
                                        openPct = float(abData['Open'])/float(abData['Delivered']) * 100.0
                                        spamPct = float(abData['Spam'])/float(abData['Delivered']) * 100.0
                                        uniqueOpenPct = float(uniAb['Unique_Open'])/float(abData['Delivered']) * 100.0
                                        uniqueClickPct = float(uniAb['Unique_Click'])/float(abData['Delivered']) * 100.0
                                    numString += "\"" + str(variantSL) + " --Variant " + str(abData['ExperimentVariant']) + "\","

                                    numString += str(messageStartDate) + ","
                                    numString += str(abData['Sent']) + ","
                                    numString += str(abData['Delivered']) + ","
                                    numString += str(delivPct)[:4] + "%,"
                                    numString += str(abData['Open']) + ","
                                    numString += str(openPct)[:4] + "%,"
                                    numString += str(uniAb['Unique_Open']) + ","
                                    numString += str(uniqueOpenPct)[:4] + "%,"
                                    numString += str(uniAb['Unique_Click']) + ","
                                    numString += str(uniqueClickPct)[:4] + "%,"
                                    numString += str(abData['Bounce']) + ","
                                    numString += str(bouncePct)[:4] + "%,"
                                    numString += str(abData['Dropped']) + ","
                                    numString += str(abData['Unsubscribe']) + ","
                                    numString += str(abData['Spam']) + ","
                                    numString += str(spamPct)[:4] + "%,"
                                    numString += str(categoryName) + ","
                                    numString += "https://www.leanplum.com/dashboard?appId=" +  str(appBundle['AppId']) + "#/" + str(appBundle['AppId']) + "/messaging/" + str(abData['MessageId']) + "\n"

                                    file.write(numString.encode('utf-8'))
                                    numString = ""
                                    #print("Writing : : : " + str(abData['ExperimentVariant']),flush=True)
                                #Finished looping over AB Variants
                                break

                            else:
                            
                                delivPct = 0.0
                                bouncePct = 0.0
                                openPct = 0.0
                                uniqueOpenPct = 0.0
                                uniqueClickPct = 0.0
                                spamPct = 0.0

                                if(float(item['Sent']) > 0.0):
                                    delivPct = float(item['Delivered'])/float(item['Sent']) * 100.0
                                    bouncePct = float(item['Bounce'])/float(item['Sent']) * 100.0
                                if(float(item['Delivered']) > 0.0):
                                    openPct = float(item['Open'])/float(item['Delivered']) * 100.0
                                    spamPct = float(item['Spam'])/float(item['Delivered']) * 100.0
                                    uniqueOpenPct = float(uni['Unique_Open'])/float(item['Delivered']) * 100.0
                                    uniqueClickPct = float(uni['Unique_Click'])/float(item['Delivered']) * 100.0
                                numString += "\"" + item['Subject'] + "\","
                                #Removing MessageID as Excel malforms it.
                                #numString += str(item['MessageId']) + ","
                                numString += str(messageStartDate) + ","
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
                                numString += str(item['Spam']) + ","
                                numString += str(spamPct)[:4] + ","
                                numString += str(categoryName) + ","
                                numString += "https://www.leanplum.com/dashboard?appId=" +  str(appBundle['AppId']) + "#/" + str(appBundle['AppId']) + "/messaging/" + str(item['MessageId']) + "\n"


                                file.write(numString.encode('utf-8'))
                                break
                file.close() 

                #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                lineCount = 0
                p = subprocess.Popen(['wc','-l',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                result, err = p.communicate()
                if p.returncode != 0:
                    print("\t\tINFO: Error reading end file",flush=True)
                else:
                    lineCount = int(result.strip().split()[0])

                if(lineCount == 1):
                    print("\t\tINFO: Zero Records Returned. Deleting Report.",flush=True)
                    os.remove(fileName)
                else:
                    print("\t\tSuccess",flush=True)
                file.close()
            
            except googleapiclient.errors.HttpError as inst:
                print("\t\tWarning: This App had bad query. Deleting Report. " + str(type(inst)),flush=True)
                file.close()
                os.remove(fileName)
                pass
            print("\t\tCleaning up dataset . . ",end="",flush=True)
            #delete_generic_table(client=bq_client, table="Email_Message_Ids_" + str(appBundle['AppId']), dataset='email_report_backups')
            print("Clean",flush=True)

        print("Finished Running Reports")
    #Domain Report
    elif(reportType == 'd'):
            print('\tCreating report by Domain against : ' + Domains,flush=True)

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
                    print("\n\tRunning Report on App :: " + str(app['AppName']) + ":" + str(app['AppId']),flush=True)

                    load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(app['AppId']))

                    fileName = "./Reports/EmailData_" + str(app['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + "_domain.csv"
                    directory = os.path.dirname(fileName)
                    if not os.path.exists(directory):
                        os.makedirs(directory)
                    file = open(fileName, "wb")
                    file.write("MessageName,SenderDomain,Domain,StartDate,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,Spam,Spam_PCT,Type,Category,MessageLink\n".encode('utf-8'))

                    attrLoc = ''

                    #Look up email attr in datastore
                    appId = int(app['AppId'])
                    #Create datastore entity
                    ds_client = datastore.Client(project='leanplum')
                    query = ds_client.query(kind='App')
                    key = ds_client.key('App',appId)
                    query.key_filter(key,'=')

                    emailName = ''
                    emailLoc = 0
                    categoryList = []

                    #Do Query on Datastore
                    print("\t\tTapping Datstore:App", flush=True)
                    appList = list(query.fetch())

                    try:
                    #Should only return the AppData for appId specific in key
                        if(len(appList)!= 1):
                            print('\t\tBad App Entities returned from AppID for ' + str(app['app_AppName']) + '.Ignore for Unwanted Apps',flush=True)
                        else:
                            emailName = dict(appList[0])['email_user_attribute']
                            #Run query against app data to find location of email attr
                            query = ds_client.query(kind='AppData')
                            key = ds_client.key('AppData',appId)
                            query.key_filter(key,'=')

                            print("\t\tTapping Datastore:AppData", flush=True)
                            appDataList = list(query.fetch())
                            try:
                                categoryList = appList[0]['unsubscribe_categories']
                            except KeyError:
                                print("\t\tINFO: App has no email categories", flush=True)

                            if(len(appDataList) != 1):
                                print('\t\tBad AppData Entities returned from AppID for ' + str(app['app_AppName']) + '.Ignore for Unwanted Apps',flush=True)
                            else:
                                #Count rows to find email location
                                attrColumns = dict(appDataList[0])['attribute_columns']
                                for attr in attrColumns:
                                    if(attr == emailName):
                                        break
                                    else:
                                        emailLoc = emailLoc + 1

                    except KeyError:
                        print("\t\tWarning: This App doesn't have email attribute specified on App *OR* in user attributes list.",flush=True)
                        pass
                    #Set emailLocation to string - Lazy
                    attrLoc = str(emailLoc)
                    print('\t\tEmail Name : ' + emailName + ' : at Location : ' + attrLoc, flush=True)

                    domainQuery = DomainGenerator.create_domain_line_query(startDate, endDate, str(app['AppId']), attrLoc)
                    print("\t\tRunning Query for Domain", flush=True)
                    domainJob = bq_client.query(domainQuery)
                    bq_client.wait_for_job(domainJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    domainResults = bq_client.get_query_rows(domainJob[0])

                    domainUniqueQuery = DomainGenerator.create_unique_domain_query(startDate, endDate, str(app['AppId']), attrLoc)
                    print("\t\tRunning Query for Uniques", flush=True)
                    domainUniJob = bq_client.query(domainUniqueQuery)
                    bq_client.wait_for_job(domainUniJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    domainUniResults = bq_client.get_query_rows(domainUniJob[0])

                    senderEmailQuery = create_sender_email_query(startDate, endDate)
                    print("\t\tRunning Query for Sender Emails", flush=True)
                    senderJob = bq_client.query(senderEmailQuery)
                    bq_client.wait_for_job(senderJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    senderEmailResults = bq_client.get_query_rows(senderJob[0])

                    defaultEmailSenderQuery = create_default_sender_email_query(str(app['AppId']), str(endDate))
                    print("\t\tRunning Query for Default Sender Email", flush=True)
                    defaultEmailJob = bq_client.query(defaultEmailSenderQuery)
                    bq_client.wait_for_job(defaultEmailJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    defaultEmail = bq_client.get_query_rows(defaultEmailJob[0])[0]['email_from_address']

                    #Used for All Category -- keep running track of value for messageId
                    allCategoryDict = {'MessageName':'','MessageId':0,'SenderDomain':'','Domain':'All','Sent':0,'Delivered':0,'Open':0,'Unique_Open':0,'Unique_Click':0,'Bounce':0,'Dropped':0,'Unsubscribe':0,'Spam':0,'Type':'','Category':'Default','MessageLink':''}

                    #Loop through all results and build report
                    for domainNum in domainResults:
                        for domainUni in domainUniResults:
                            if(str(domainNum['Domain']) == str(domainUni['Domain']) and str(domainNum['MessageId']) == str(domainUni['MessageId'])):
                                if(int(domainNum['Sent']) == 0 or int(domainNum['ID']) != 1):
                                    if(int(domainNum['Sent']) == 0):
                                        print("\t\tINFO: Skipping MessageId :: " + str(domainNum['MessageId']) + " :: on domain :: " + str(domainNum['Domain']) + " :: due to no `Sent` events for time range",flush=True)
                                    elif(int(domainNum['ID']) != 1):
                                        print("\t\tINFO: Skipping Duplicate Message Row :: " + str(domainNum['MessageId']) + " :: due to multiple ID's availble. --> ID's == " + str(domainNum['ID']),flush=True)
                                    break

                                numString = ""
                                senderEmail = ""

                                #Look for the sender email
                                for senderDict in senderEmailResults:
                                    if str(senderDict['MessageId']) == str(domainNum['MessageId']):
                                        senderEmail = senderDict['SenderEmail']
                                if( len(senderEmail) == 0 ):
                                    senderEmail = defaultEmail
                                delivPct = 0.0
                                bouncePct = 0.0
                                openPct = 0.0
                                uniqueOpenPct = 0.0
                                uniqueClickPct = 0.0
                                spamPct = 0.0

                                if(float(domainNum['Sent']) > 0.0):
                                    delivPct = float(domainNum['Delivered'])/float(domainNum['Sent']) * 100.0
                                    bouncePct = float(domainNum['Bounce'])/float(domainNum['Sent']) * 100.0
                                if(float(domainNum['Delivered']) > 0.0):
                                    openPct = float(domainNum['Open'])/float(domainNum['Delivered']) * 100.0
                                    spamPct = float(domainNum['Spam'])/float(domainNum['Delivered']) * 100.0
                                    uniqueOpenPct = float(domainUni['Unique_Open'])/float(domainNum['Delivered']) * 100.0
                                    uniqueClickPct = float(domainUni['Unique_Click'])/float(domainNum['Delivered']) * 100.0

                                if(allCategoryDict['MessageId'] == 0):
                                    allCategoryDict['MessageId'] = domainNum['MessageId']
                                elif(allCategoryDict['MessageId'] != domainNum['MessageId']):
                                    #Grab messageId startDate
                                    messageStartDate = ""
                                    categoryId = -1
                                    categoryName = 'Default'
                                    ds_client = datastore.Client(project='leanplum')
                                    query = ds_client.query(kind='Study')
                                    key = ds_client.key('Study',int(allCategoryDict['MessageId']))
                                    query.key_filter(key,'=')
                                    qList = list(query.fetch())
                                    ## INFO 
                                        # [0] :: Get the payload from the query (There's only one)
                                        # ['active_since'] :: Payload is a dictionary
                                        # .dat() :: In this case a datetime object is returned
                                    try:
                                        if( str(qList[0]['action_type']) != "__Email" ):
                                            print("\t\tWarning: Captured wrong message :: " + str(domainNum['MessageId']) + " :: Type = " + str(qList[0]['action_type']), flush=True)
                                            break
                                        messageStartDate = str(qList[0]['active_since'].date())
                                        try:
                                            categoryId = int(qList[0]['category_id'])
                                            for categoryDict in categoryList:
                                                if(categoryId == int(categoryDict['id'])):
                                                    categoryName = str(categoryDict['name'])
                                                    allCategoryDict['Category'] = categoryName
                                            break
                                        except TypeError:
                                            #message is in the default category
                                            pass
                                    except IndexError:
                                        pass
                                    except AttributeError:
                                        pass

                                    if(messageStartDate == ""):
                                        print("\t\t\tDataStore has no record of MessageId STUDY:: " + str(domainNum['MessageId']),flush=True )
                                        messageStartDate = "Unknown"                 
        
                                    #Aggregate
                                    try:
                                        allStr = ''
                                        allStr += allCategoryDict['MessageName'] + ','
                                        allStr += str(allCategoryDict['SenderDomain']) + ','
                                        allStr += str(allCategoryDict['Domain']) + ','
                                        allStr += str(messageStartDate) + ','
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
                                        allStr += str(allCategoryDict['Spam']) + ','
                                        allStr += str(float(allCategoryDict['Spam'])/float(allCategoryDict['Delivered']) * 100.0)[:4] + '%,'
                                        allStr += str(allCategoryDict['Type']) + ','
                                        allStr += str(allCategoryDict['Category']) + ','
                                        allStr += ' \n'

                                        #Don't Write If Nothing There
                                        if(allCategoryDict['Sent'] != 0):
                                            file.write(allStr.encode('utf-8'))
                                        else:
                                            print("\t\tINFO: Not Writing `All` Row for due to no no `Sends` for MessageId :: " + str(allCategoryDict['MessageId']),flush=True)

                                    except ZeroDivisionError:
                                        pass
                                    #Zero out and Update
                                    allCategoryDict = {'MessageName':'','MessageId':0,'SenderDomain':'','Domain':'All','Sent':0,'Delivered':0,'Open':0,'Unique_Open':0,'Unique_Click':0,'Bounce':0,'Dropped':0,'Unsubscribe':0,'Spam':0,'Type':'', 'Category':'Default','MessageLink':''}
                                    allCategoryDict['MessageId'] = domainNum['MessageId']

                                numString += "\"" + domainNum['MessageName'] + " (" + senderEmail +  ")\","
                                allCategoryDict['MessageName'] = "\"" + domainNum['MessageName'] + " (" + senderEmail +  ")\""

                                prefix = re.search(".*@",senderEmail).group(0)
                                domain = senderEmail[len(prefix):]
                                numString += str(domain) + ","
                                allCategoryDict['SenderDomain'] = str(domain)

                                #Removing Message ID as Excel Malforms
                                #numString += str(domainNum['MessageId']) + ","
                                numString += str(domainNum['Domain']) + ","

                                #Grab messageId startDate. #### NOT THE MOST IDEAL PLACE FOR THIS BUT OH WELL ####
                                messageStartDate = ""
                                categoryId = -1
                                categoryName = 'Default'
                                ds_client = datastore.Client(project='leanplum')
                                query = ds_client.query(kind='Study')
                                key = ds_client.key('Study',int(domainNum['MessageId']))
                                query.key_filter(key,'=')
                                qList = list(query.fetch())
                                ## INFO 
                                    # [0] :: Get the payload from the query (There's only one)
                                    # ['active_since'] :: Payload is a dictionary
                                    # .date() :: In this case a datetime object is returned
                                try:
                                    if( str(qList[0]['action_type']) != "__Email" ):
                                        print("\t\tWarning: Captured wrong message :: " + str(domainNum['MessageId']) + " :: Type = " + str(qList[0]['action_type']), flush=True)
                                        break
                                    messageStartDate = str(qList[0]['active_since'].date())
                                    try:
                                        categoryId = int(qList[0]['category_id'])
                                        for categoryDict in categoryList:
                                            if(categoryId == int(categoryDict['id'])):
                                                categoryName = str(categoryDict['name'])
                                            break
                                    except TypeError:
                                        #message doesn't have cateogry
                                        pass
                                except IndexError:
                                    pass
                                except AttributeError:
                                    pass

                                if(messageStartDate == ""):
                                    print("\t\t\tDataStore has no record of MessageId STUDY:: " + str(domainNum['MessageId']),flush=True )
                                    messageStartDate = "Unknown"

                                numString += str(messageStartDate) + ","

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

                                numString += str(domainNum['Spam']) + ","
                                allCategoryDict['Spam'] += domainNum['Spam']

                                numString += str(spamPct)[:4] + "%,"

                                numString += str(domainNum['Type']) + ","
                                allCategoryDict['Type'] = str(domainNum['Type'])

                                numString += str(categoryName) + ","

                                numString += "https://www.leanplum.com/dashboard?appId=" +  str(app['AppId']) + "#/" + str(app['AppId']) + "/messaging/" + str(domainNum['MessageId']) + "\n"

                                file.write(numString.encode('utf-8'))
                                break

                    file.close()
                    #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                    lineCount = 0
                    p = subprocess.Popen(['wc','-l',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    result, err = p.communicate()
                    if p.returncode != 0:
                        print("\t\tINFO: Error reading end file",flush=True)
                    else:
                        lineCount = int(result.strip().split()[0])

                    if(lineCount == 1):
                        print("\t\tINFO: Zero Records Returned. Deleting Report",flush=True)
                        os.remove(fileName)
                    else:
                        print("\t\tSuccess",flush=True)
                    file.close()
                except googleapiclient.errors.HttpError as inst:
                    print("\t\tWarning: This App had bad query. Deleting Report. " + str(type(inst)),flush=True)
                    file.close()
                    os.remove(fileName)
                    pass

                print("\t\tCleaning up dataset . . ",end="",flush=True)
                delete_generic_table(client=bq_client, table="Email_Message_Ids_" + str(app['AppId']), dataset='email_report_backups')
                print("Clean",flush=True)

            #attrFile.close()
            print("Finished Running Reports")
    #Push Report
    elif(reportType == 'p'):

            #Lookup App Id's for the company
            appidsQuery = create_appids_query(companyId, endDate)
            appJob = bq_client.query(appidsQuery)
            bq_client.wait_for_job(appJob[0],timeout=120)
            appResults = bq_client.get_query_rows(appJob[0])

            #Loop through all App's gathered
            for app in appResults:
                #In case the query fails because of missing data or a test app
                try:
                    print("\n\tRunning Report on App :: " + str(app['AppName']) + ":" + str(app['AppId']))
                    fileName = "./Reports/PushData_" + str(app['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + ".csv"
                    directory = os.path.dirname(fileName)
                    if not os.path.exists(directory):
                        os.makedirs(directory)
                    file = open(fileName, "wb")
                    file.write("MessageName,StartDate,Sent,Open,Open_PCT,Held Back,Bounce,Bounce_PCT,MessageLink\n".encode('utf-8'))

                    pushQuery = PushGenerator.create_push_notification_query(startDate, endDate, str(app['AppId']))
                    pushJob = bq_client.query(pushQuery)
                    print("\t\tRunning Query for Push", flush=True)
                    bq_client.wait_for_job(pushJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    pushResults = bq_client.get_query_rows(pushJob[0])

                    pushNameQuery = PushGenerator.create_push_message_id_with_name_query(startDate, endDate, str(app['AppId']))
                    pushNameJob = bq_client.query(pushNameQuery)
                    print("\t\tRunning Query for Push Names", flush=True)
                    bq_client.wait_for_job(pushNameJob[0],timeout=120)
                    print("\t\tQuery Success", flush=True)
                    pushNameResults = bq_client.get_query_rows(pushNameJob[0])

                    #Loop through results and build report
                    for pushRows in pushResults:
                        for pushName in pushNameResults:
                            if pushRows['MessageId'] == pushName['MessageId']:
                                if(int(pushRows['Sent']) == 0):
                                    break
                                else:

                                    #Grab messageId startDate
                                    messageStartDate = ""
                                    datastorePushName = ""
                                    ds_client = datastore.Client(project='leanplum')
                                    query = ds_client.query(kind='Study')
                                    key = ds_client.key('Study',int(pushRows['MessageId']))
                                    query.key_filter(key,'=')
                                    qList = list(query.fetch())
                                    ## INFO 
                                        # [0] :: Get the payload from the query (There's only one)
                                        # ['active_since'] :: Payload is a dictionary
                                        # .dat() :: In this case a datetime object is returned
                                    try:
                                        if( str(qList[0]['action_type']) != "__Push Notification" ):
                                            print("\t\tWarning: Captured wrong message :: " + str(pushRows['MessageId']) + " :: Type = " + str(qList[0]['action_type']), flush=True)
                                            break
                                        datastorePushName = str(qList[0]['name'])
                                        messageStartDate = str(qList[0]['active_since'].date())
                                    except IndexError:
                                        pass
                                    except AttributeError:
                                        pass

                                    if(messageStartDate == ""):
                                        print("\t\t\tDataStore has no record of MessageId STUDY:: " + str(pushRows['MessageId']),flush=True )
                                        messageStartDate = "Unknown"


                                    openPct = float(pushRows['Open'])/float(pushRows['Sent']) * 100.0
                                    bouncePCT = float(pushRows['Bounce'])/float(pushRows['Sent']) * 100.0

                                    numString = ""

                                    if(datastorePushName == ""):
                                        numString += "\"" + str(pushName['Name']) + "\","
                                    else:
                                        numString += "\"" + str(datastorePushName) + "\","
                                    numString += str(messageStartDate) + ","
                                    numString += str(pushRows['Sent']) + ","
                                    numString += str(pushRows['Open']) + ","
                                    numString += str(openPct)[:4] + "%,"
                                    numString += str(pushRows['Held_Back']) + ","
                                    numString += str(pushRows['Bounce']) + ","
                                    numString += str(bouncePCT)[:4] + "%,"

                                    numString += "https://www.leanplum.com/dashboard?appId=" + str(app['AppId']) + "#/" + str(app['AppId']) + "/messaging/" + str(pushRows['MessageId']) + "\n"

                                    file.write(numString.encode('utf-8'))
                                    #Since we are in two for loops we break here since we already matched the name we don't need to continue through the loop
                                    break
                    file.close()
                    #Clean up zero records for valid queries
                    lineCount = 0
                    p = subprocess.Popen(['wc','-l',fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    result,err = p.communicate()
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
                    print("\t\tWarning: This App had a bad query. Deleting Report. " + str(type(inst)))
                    file.close()
                    os.remove(fileName)
                    pass
            print("Finished Running Reports")
                    