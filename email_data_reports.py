# Copyright 2018, Leanplum Inc.
# Author: Avery Tang (avery.tang@leanplum.com)
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

#List of domains to breakout
Domains = "(\"gmail.com\",\"msn.com\",\"hotmail.com\", \"yahoo.com\")"

def retrieve_backup_files(service, date, bucket):
    """retrieve all datastore backup file names for the date supplied

    :param service: object, googleapiclient
    :param date: str, data backup date in %Y%m%d format, e.g 20170425
    :param bucket, google storage bucket name
    :return: a list of file names
    """

    search_str = "backup_" + date  # 20170313
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return, prefix=search_str)

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
    :param project: str, project id
    :param dataset: str, name for the data set where the table will be created
    :param model: str, datastore model
    :return: None
    """

    # logger.info("Retrieving backup files for {} on {}...".format(model, date))
    if( date < "2018-04-26" ):
        files = retrieve_backup_files(service, date, bucket)
        model_search_str = "." + model.lower() + "."
        backup_file = [x for x in files if model_search_str in x.lower()]

        if len(backup_file) == 1:
            source_uri = 'gs://' + bucket + "/" + backup_file[0]
            table_name = model.title() + "_" + date
            existing_tables = [x for x in client.get_all_tables(dataset) if model.title() + "_" in x]

            loading = client.import_data_from_uris(source_uris=source_uri,
                                                   dataset=dataset,
                                                   table=table_name,
                                                   source_format='DATASTORE_BACKUP')

            job_id = loading['jobReference']['jobId']
            print("Loading Model : " + model + "_backup - " + date, flush=True)
            job = client.wait_for_job(job_id, timeout=600)
            print("Model Loaded : " + model + "_backup - " + date, flush=True)
    else:
        files = retrieve_backup_files(service,date,"leanplum_datastore_backups")

#This Query is intentionally wrong. When Querying against the study table, I have added the eventtime and am grouping on it. THis 
# is to ensure that we count unique's per day not per message. This is wrong BUT it is what our analytics shows and we would rather
# be consistant than right.
def create_domain_unique_query(appId, startDate, endDate, attrLoc):
    query = """
        --MultiLine
    SELECT
      Dom.MessageID as MessageID,
      Dom.Domain as Domain,
      SUM(IF(Dom.Event = "Open", Dom.Occur,0)) AS Unique_Open,
      SUM(IF(Dom.Event = "Click", Dom.Occur,0)) AS Unique_Click
    FROM
        (SELECT
          Sessions.Ses_Message_ID AS MessageID,
          CASE
            WHEN (REGEXP_EXTRACT(Sessions.Email, r'@(.+)') NOT IN """ + Domains + """ OR REGEXP_EXTRACT(Sessions.Email, r'@(.+)') IS NULL ) THEN "Other" 
            ELSE REGEXP_EXTRACT(Sessions.Email, r'@(.+)')
          END AS Domain,
          CASE
            WHEN Sessions.Ses_Event="" THEN "Sent"
            ELSE Sessions.Ses_Event
          END AS Event,
          COUNT(Sessions.Ses_Event) AS Occur
        FROM
          (SELECT
            user_id,
            INTEGER(SUBSTR(states.events.name,3,16)) AS Ses_Message_ID,
            attr"""+ attrLoc + """ as Email,
            SUBSTR(states.events.name, 20) AS Ses_Event,
            SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS eventtime
          FROM
            (TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
              TIMESTAMP('""" + startDate + """'),
              TIMESTAMP('""" + endDate + """')))
          WHERE states.events.name LIKE ".m%"
          GROUP BY user_id, Ses_Message_ID, Ses_Event, Email, eventtime) Sessions
        JOIN
          (SELECT
            app.id,
            __key__.id as Study_Message_ID,
          FROM
            (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
              TIMESTAMP('""" + startDate + """'),
              TIMESTAMP('""" + endDate + """')))
          WHERE action_type = "__Email" AND app.id = INTEGER(\"""" + appId + """\")
          GROUP BY app.id, Study_Message_ID) Study
        ON Study.Study_Message_ID = Sessions.Ses_Message_ID
        GROUP BY MessageID, Domain, Event) AS Dom
    GROUP BY MessageID, Domain
    ORDER BY MessageID
    """

    return query

def create_domain_line_query(appId, startDate, endDate, attrLoc):
    query = """
        --Multiline
    SELECT
        A.MessageName as MessageName,
        B.MessageID as MessageID,
        B.Domain as Domain,
        B.Sent as Sent,
        B.Delivered as Delivered,
        B.Open as Open,
        B.Click as Click,
        B.Bounce as Bounce,
        B.Dropped as Dropped,
        B.Block as Block,
        B.Unsubscribe as Unsubscribe,
        ROW_NUMBER() OVER(PARTITION BY B.MessageID, B.Domain ORDER BY A.Time DESC) AS ID,
        A.Type as Type,
    FROM
    (SELECT
        last_change_time as time,
        name as MessageName,
        __key__.id as MessageId,
        CASE
          WHEN delivery_type = INTEGER("0") THEN "Immediate"
          WHEN delivery_type = INTEGER("1") THEN "Future"
          WHEN delivery_type = INTEGER("2") THEN "Manual"
          WHEN delivery_type = INTEGER("3") THEN "Triggered Local"
          WHEN delivery_type = INTEGER("4") THEN "Triggered Server"
          WHEN delivery_type = INTEGER("5") THEN "Recurring"
          ELSE "Unknown"
        END AS Type
    FROM
        (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
            TIMESTAMP('""" + startDate + """'),
            TIMESTAMP('""" + endDate + """')))) A
    JOIN
    (SELECT
      Fin.MessageID as MessageID,
      Fin.Domain as Domain,
      SUM(IF(Fin.Event = "Sent", Fin.Occur,0)) AS Sent,
      SUM(IF(Fin.Event = "Delivered", Fin.Occur,0)) AS Delivered,
      SUM(IF(Fin.Event = "Open", Fin.Occur,0)) AS Open,
      SUM(IF(Fin.Event = "Click", Fin.Occur,0)) AS Click,
      SUM(IF(Fin.Event = "Bounce", Fin.Occur,0)) AS Bounce,
      SUM(IF(Fin.Event = "Dropped", Fin.Occur,0)) AS Dropped,
      SUM(IF(Fin.Event = "Block", Fin.Occur,0)) AS Block,
      SUM(IF(Fin.Event = "Unsubscribe", Fin.Occur,0)) AS Unsubscribe,
    FROM
      (SELECT
        Dom.MessageID AS MessageID,
        Dom.Domain AS Domain,
        Dom.Sessions.Ses_Event AS Event,
        SUM(Dom.Occur) AS Occur
      FROM
        (SELECT
          Sessions.Ses_Message_ID as MessageID,
          CASE
            WHEN (REGEXP_EXTRACT(Sessions.Email, r'@(.+)') NOT IN """ + Domains + """ OR REGEXP_EXTRACT(Sessions.Email, r'@(.+)') IS NULL ) THEN "Other" 
            ELSE REGEXP_EXTRACT(Sessions.Email, r'@(.+)')
          END AS Domain,
          CASE
            WHEN Sessions.Ses_Event="" THEN "Sent"
            ELSE Sessions.Ses_Event
          END AS Sessions.Ses_Event,
          Ses_Total_Occur AS Occur
        FROM
          (SELECT
            INTEGER(SUBSTR(states.events.name,3,16)) AS Ses_Message_ID,
            attr"""+ attrLoc + """ as Email,
            SUBSTR(states.events.name, 20) AS Ses_Event,
            COUNT(*) as Ses_Total_Occur
          FROM
            (TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
              TIMESTAMP('""" + startDate + """'),
              TIMESTAMP('""" + endDate + """')))
          WHERE states.events.name LIKE ".m%"
          GROUP BY Ses_Message_ID, Ses_Event, Email) Sessions
        JOIN
          (SELECT
            app.id,
            __key__.id as Study_Message_ID,
          FROM
            (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
              TIMESTAMP('""" + startDate + """'),
              TIMESTAMP('""" + endDate + """')))
          WHERE action_type = "__Email" AND app.id = INTEGER(\"""" + appId + """\")
          GROUP BY app.id, Study_Message_ID) Study
        ON Study.Study_Message_ID = Sessions.Ses_Message_ID) AS Dom
      GROUP BY MessageID, Domain, Event) Fin
    GROUP BY MessageID, Domain
    ORDER BY MessageID) B
    ON A.MessageId = B.MessageID
    GROUP BY MessageName, MessageID, B.MessageID, Domain, B.Domain, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe, A.time, Type
    ORDER BY MessageID, Domain
    """
    return query

def create_subject_line_query(companyId, appId, startDate, endDate):
    query = """
    --Last) Merge
    SELECT
        Subject.subject as Subject,
        Subject.messageid as MessageId,
        Message.Sent as Sent,
        Message.Delivered as Delivered,
        Message.Open as Open,
        Message.Click as Click,
        Message.Bounce as Bounce,
        Message.Dropped as Dropped,
        Message.Block as Block,
        Message.Unsubscribe as Unsubscribe
    FROM
    --Fifth) Get all subject lines 
        (SELECT 
            vars.value.text as subject,
            study.id as messageid
        FROM
            (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
                TIMESTAMP('""" + startDate + """'),
                TIMESTAMP('""" + endDate + """'))) 
        WHERE vars.name="Subject") Subject
    JOIN
    --Fourth) Pivot Table (Get Sums)
        (SELECT
            Sessions.ses_Messageid as Messageid,
            SUM(IF(Sessions.ses_event="Sent", Sessions.ses_total_occur, 0)) AS Sent,
            SUM(IF(Sessions.ses_event="Delivered", Sessions.ses_total_occur, 0)) AS Delivered,
            SUM(IF(Sessions.ses_event="Open", Sessions.ses_total_occur, 0)) AS Open,
            SUM(IF(Sessions.ses_event="Click", Sessions.ses_total_occur, 0)) AS Click,
            SUM(IF(Sessions.ses_event="Bounce", Sessions.ses_total_occur, 0)) AS Bounce,
            SUM(IF(Sessions.ses_event="Dropped", Sessions.ses_total_occur, 0)) AS Dropped,
            SUM(IF(Sessions.ses_event="Block", Sessions.ses_total_occur, 0)) AS Block,
            SUM(IF(Sessions.ses_event="Unsubscribe", Sessions.ses_total_occur, 0)) AS Unsubscribe
        FROM(
    --Third)Get userId's, messageId's, and message types that are in the message list for this appId
            SELECT 
                Sessions.ses_Messageid,
                CASE
                    WHEN Sessions.ses_event="" THEN "Sent"
                    ELSE Sessions.ses_event
                    END AS Sessions.ses_event,
                Sessions.ses_total_occur
            FROM
    --All messages for AppID
                (SELECT
    --user_id, might need to group by since userid's are repeated
                    INTEGER(SUBSTR(states.events.name,3,16)) AS ses_Messageid,
                    SUBSTR(states.events.name, 20) AS ses_event,
                    count(*) as ses_total_occur
                FROM
    --(Script will place in the appid and TIME_DATE_RANGE the range of sessions)
                    (TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
                        TIMESTAMP('""" + startDate + """'),
                        TIMESTAMP('""" + endDate + """')))
                WHERE states.events.name LIKE ".m%"
                GROUP BY ses_Messageid, ses_event) Sessions
            INNER JOIN
                (SELECT 
                    Study.study_MessageID
                FROM
    --First)Get App Id's from Company Id's
                    (SELECT
                        company.id as app_CompanyID,
                        name as app_AppName,
                        __key__.id as app_AppID
                    FROM 
    --Probably only need the most recent datetime.today()
                        [leanplum-staging:email_report_backups.App_""" + endDate + """]
                    WHERE STRING(company.id) ='""" + companyId + """') App
                INNER JOIN
    --Second)Get Email Message Id's from Appid's. (Use appids in python script for third query)
                    (SELECT
                        app.id as study_AppId,
                        __key__.id as study_MessageID
                    FROM
                        (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
                            TIMESTAMP('""" + startDate + """'),
                            TIMESTAMP('""" + endDate + """'))) 
                    WHERE action_type = "__Email" ) Study
                ON App.app_AppId = Study.study_AppId
                GROUP BY Study.study_MessageID) Sub
            ON Sessions.ses_Messageid = Sub.Study.study_MessageID
            ORDER BY Sessions.ses_Messageid, Sessions.ses_event)
        GROUP BY Messageid) Message
    ON Subject.messageid = Message.Messageid
    GROUP BY Subject, MessageId, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe
    """
    return query

def create_subject_line_uniq_query(companyId, appId, startDate, endDate):
    query = """
    --Grab Subject
    SELECT
        Subject.subject as Subject,
        Subject.messageid as MessageId,
        Message.Unique_Open as Unique_Open,
        Message.Unique_Click as Unique_Click
    FROM
        --Fifth) Get all subject lines 
        (SELECT 
            vars.value.text as subject,
            study.id as messageid
        FROM
            (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
                TIMESTAMP('""" + startDate + """'),
                TIMESTAMP('""" + endDate + """')))
        WHERE vars.name="Subject") Subject
    JOIN
        --Fourth) Pivot Table (Get Sums)
        (SELECT
            Sessions.ses_Messageid as Messageid,
            SUM(IF(Sessions.ses_event="Open", unique_total, 0)) AS Unique_Open,
            SUM(IF(Sessions.ses_event="Click", unique_total, 0)) AS Unique_Click
        FROM(
            --Third)Get userId's, messageId's, and message types that are in the message list for this appId
            SELECT 
                Sessions.ses_Messageid,
                CASE
                    WHEN Sessions.ses_event="" THEN "Sent"
                    ELSE Sessions.ses_event
                    END AS Sessions.ses_event,
                COUNT(Sessions.ses_event) as unique_total
            FROM
                --All messages for AppID
                (SELECT
                    user_id,
                    INTEGER(SUBSTR(states.events.name,3,16)) AS ses_Messageid,
                    SUBSTR(states.events.name, 20) AS ses_event,
                    count(*) as ses_total_occur,
                    SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS eventtime
                FROM
                    --(Script will place in the appid and TIME_DATE_RANGE the range of sessions)
                    (TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
                        TIMESTAMP('""" + startDate + """'),
                        TIMESTAMP('""" + endDate + """')))
                        WHERE states.events.name LIKE ".m%"
                GROUP BY user_id, ses_Messageid, ses_event, eventtime) Sessions
            INNER JOIN
                (SELECT Study.study_MessageID
                FROM
                    --First)Get App Id's from Company Id's
                    (SELECT
                        company.id as app_CompanyID,
                        name as app_AppName,
                        __key__.id as app_AppID
                    FROM 
                        --Probably only need the most recent datetime.today()
                        [leanplum-staging:email_report_backups.App_""" + endDate + """]
                    WHERE STRING(company.id) ='""" + companyId + """') App
                INNER JOIN
                    --Second)Get Email Message Id's from Appid's. (Use appids in python script for third query)
                    (SELECT
                        app.id as study_AppId,
                        __key__.id as study_MessageID
                    FROM
                        (TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
                            TIMESTAMP('""" + startDate + """'),
                            TIMESTAMP('""" + endDate + """')))  
                    WHERE action_type = "__Email" ) Study
                ON App.app_AppId = Study.study_AppId
                GROUP BY Study.study_MessageID) Sub
            ON Sessions.ses_Messageid = Sub.Study.study_MessageID
            GROUP BY Sessions.ses_Messageid, Sessions.ses_event
            ORDER BY Sessions.ses_Messageid, Sessions.ses_event)
        GROUP BY Messageid) Message
    ON Subject.messageid = Message.Messageid
    GROUP BY Subject, MessageId, Unique_Open, Unique_Click
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

if __name__ == "__main__":

    #Parse command line inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-p', help='project id', default='leanplum-staging')
    parser.add_argument('--dataset', '-d',  help='dataset that stores the tables', default="email_report_backups")
    parser.add_argument('--model', '-m', help='datastore model(s) to load', nargs='+', default=['App','Study','Experiment'])
    #We do not catch bad date format
    parser.add_argument('--dateS', '-ts', help='start date YYYYMMDD', required=True)
    parser.add_argument('--dateE', '-te', help='end date YYYYMMDD', required=True)
    parser.add_argument('--bucket', '-b', help='google storage bucket name', default='leanplum_backups')
    parser.add_argument('--company', '-c', help='company id', required=True)
    parser.add_argument('--report', '-r', help='report type (s)ubject/(d)domain', required=True)
    args = parser.parse_args()

    # initialize google and bq clients
    google_credential = GoogleCredentials.get_application_default()
    google_service = googleapiclient.discovery.build('storage', 'v1', credentials=google_credential)
    bq_client = bigquery.get_client(project_id=args.project, credentials=google_credential)

    #Load all the backups
    for model in args.model:
        print("Loading " + model, flush=True)
        load_multi_table(service=google_service,
                   client=bq_client,
                   dateStart=args.dateS,
                   dateEnd=args.dateE,
                   bucket=args.bucket,
                   dataset=args.dataset,
                   model=model)

    print("\tBackups Loaded")

    #Load Subject Report
    if(args.report == 's'):
        print('\tCreating report by Subject Line')
        appidsQuery = create_appids_query(args.company, args.dateE)

        #Create query for App Id's
        appJob = bq_client.query(appidsQuery)
        bq_client.wait_for_job(appJob[0])
        appResults = bq_client.get_query_rows(appJob[0])

        #Loop through all App Id's
        for appBundle in appResults:
            print("\n\tQuerying Data for " + appBundle['app_AppName'] + ":" + str(appBundle['app_AppID']))

            #In case the query fails because of missing data or a test app
            try:
                fileName = "EmailData_" + str(appBundle['app_AppName']) + "_" + str(args.dateS) + "_" + str(args.dateE) + "_subject.csv"
                file = open(fileName, "w+")
                file.write("Subject,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,MessageLink\n")

                subjectLineQuery = create_subject_line_query(args.company, str(appBundle['app_AppID']), args.dateS, args.dateE)
                subjectLineJob = bq_client.query(subjectLineQuery)
                print("\t\tRunning Query", flush=True)
                bq_client.wait_for_job(subjectLineJob[0])
                print("\t\tQuery Success", flush=True)
                subjectResults = bq_client.get_query_rows(subjectLineJob[0])

                uniqLineQuery = create_subject_line_uniq_query(args.company, str(appBundle['app_AppID']), args.dateS, args.dateE)
                uniqLineJob = bq_client.query(uniqLineQuery)
                print("\t\tRunning Query for Uniques", flush=True)
                bq_client.wait_for_job(uniqLineJob[0])
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
                            openPCt = 0.0
                            uniqueOpenPct = 0.0
                            uniqueClickPct = 0.0

                            if(float(item['Sent']) > 0.0):
                                delivPct = float(item['Delivered'])/float(item['Sent']) * 100.0
                                bouncePct = float(item['Bounce'])/float(item['Sent']) * 100.0
                            if(float(item['Delivered']) > 0.0):
                                openPCt = float(item['Open'])/float(item['Delivered']) * 100.0
                                uniqueOpenPct = float(uni['Unique_Open'])/float(item['Delivered']) * 100.0
                                uniqueClickPct = float(uni['Unique_Click'])/float(item['Delivered']) * 100.0

                            numString += "\"" + str(item['Subject']) + "\","
                            #Removing MessageID as Excel malforms it.
                            #numString += str(item['MessageId']) + ","
                            numString += str(item['Sent']) + ","
                            numString += str(item['Delivered']) + ","
                            numString += str(delivPct)[:4] + "%,"
                            numString += str(item['Open']) + ","
                            numString += str(openPCt)[:4] + "%,"
                            numString += str(uni['Unique_Open']) + ","
                            numString += str(uniqueOpenPct)[:4] + "%,"
                            numString += str(uni['Unique_Click']) + ","
                            numString += str(uniqueClickPct)[:4] + "%,"
                            numString += str(item['Bounce']) + ","
                            numString += str(bouncePct)[:4] + "%,"
                            numString += str(item['Dropped']) + ","
                            numString += str(item['Unsubscribe']) + ","
                            numString += "https://www.leanplum.com/dashboard?appId=" +  str(appBundle['app_AppID']) + "#/" + str(appBundle['app_AppID']) + "/messaging/" + str(item['MessageId']) + "\n"


                            file.write(numString)
                            break
                file.close() 

                #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                file = open(fileName, 'r')
                lineCount = sum(1 for line in file)
                if(lineCount == 1):
                    print("\t\tINFO: Zero Records Returned. Deleting Report.")
                    os.remove(fileName)
                else:
                    print("\t\tSuccess")
                file.close()
            
            except googleapiclient.errors.HttpError:
                print("\t\tWarning: This App had bad query. Deleting Report.")
                file.close()
                os.remove(fileName)
                pass
        print("Finished Running Reports")
    #Domain Report
    elif(args.report == 'd'):
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
            appidsQuery = create_appids_query(args.company, args.dateE)
            appJob = bq_client.query(appidsQuery)
            bq_client.wait_for_job(appJob[0])
            appResults = bq_client.get_query_rows(appJob[0])

            #Loop through all App's gathered
            for app in appResults:

                #In case the query fails because of missing data or a test app
                try:
                    print("\n\tQuerying Data for " + app['app_AppName'] + ":" + str(app['app_AppID']))
                    fileName = "EmailData_" + str(app['app_AppName']) + "_" + str(args.dateS) + "_" + str(args.dateE) + "_domain.csv"
                    file = open(fileName, "w+")
                    file.write("MessageName,SenderDomain,Domain,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,Type,MessageLink\n")

                    attrLoc = 0

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
                        print(dict(appDataList[0]))
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
                    #Set emailLocation to string - Lazy
                    attrLoc = str(emailLoc)
                    print('\t\tEmail Name and Location: ' + emailName + ' : at Location : ' + attrLoc)

                    domainQuery = create_domain_line_query(str(app['app_AppID']), args.dateS, args.dateE, attrLoc)
                    domainJob = bq_client.query(domainQuery)
                    print("\t\tRunning Query for Domain", flush=True)
                    bq_client.wait_for_job(domainJob[0])
                    print("\t\tQuery Success", flush=True)
                    domainResults = bq_client.get_query_rows(domainJob[0])

                    domainUniqueQuery = create_domain_unique_query(str(app['app_AppID']), args.dateS, args.dateE, attrLoc)
                    domainUniJob = bq_client.query(domainUniqueQuery)
                    print("\t\tRunning Query for Uniques", flush=True)
                    bq_client.wait_for_job(domainUniJob[0])
                    print("\t\tQuery Success", flush=True)
                    domainUniResults = bq_client.get_query_rows(domainUniJob[0])

                    senderEmailQuery = create_sender_email_query(args.dateS, args.dateE)
                    senderJob = bq_client.query(senderEmailQuery)
                    print("\t\tRunning Query for Sender Emails", flush=True)
                    bq_client.wait_for_job(senderJob[0])
                    print("\t\tQuery Success", flush=True)
                    senderEmailResults = bq_client.get_query_rows(senderJob[0])

                    defaultEmailSenderQuery = create_default_sender_email_query(str(app['app_AppID']), str(args.dateE))
                    defaultEmailJob = bq_client.query(defaultEmailSenderQuery)
                    print("\t\tRunning Query for Default Sender Email", flush=True)
                    bq_client.wait_for_job(defaultEmailJob[0])
                    print("\t\tQuery Success", flush=True)
                    defaultEmail = bq_client.get_query_rows(defaultEmailJob[0])[0]['email_from_address']

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
                                openPCt = 0.0
                                uniqueOpenPct = 0.0
                                uniqueClickPct = 0.0

                                if(float(domainNum['Sent']) > 0.0):
                                    delivPct = float(domainNum['Delivered'])/float(domainNum['Sent']) * 100.0
                                    bouncePct = float(domainNum['Bounce'])/float(domainNum['Sent']) * 100.0
                                if(float(domainNum['Delivered']) > 0.0):
                                    openPCt = float(domainNum['Open'])/float(domainNum['Delivered']) * 100.0
                                    uniqueOpenPct = float(domainUni['Unique_Open'])/float(domainNum['Delivered']) * 100.0
                                    uniqueClickPct = float(domainUni['Unique_Click'])/float(domainNum['Delivered']) * 100.0

                                numString += "\"" + str(domainNum['MessageName']) + " (" + senderEmail +  ")\","
                                prefix = re.search(".*@",senderEmail).group(0)
                                domain = senderEmail[len(prefix):]
                                numString += str(domain) + ","
                                #Removing Message ID as Excel Malforms
                                #numString += str(domainNum['MessageID']) + ","
                                numString += str(domainNum['Domain']) + ","
                                numString += str(domainNum['Sent']) + ","
                                numString += str(domainNum['Delivered']) + ","
                                numString += str(delivPct)[:4] + "%,"
                                numString += str(domainNum['Open']) + ","
                                numString += str(openPCt)[:4] + "%,"
                                numString += str(domainUni['Unique_Open']) + ","
                                numString += str(uniqueOpenPct)[:4] + "%,"
                                numString += str(domainUni['Unique_Click']) + ","
                                numString += str(uniqueClickPct)[:4] + "%,"
                                numString += str(domainNum['Bounce']) + ","
                                numString += str(bouncePct)[:4] + "%,"
                                numString += str(domainNum['Dropped']) + ","
                                numString += str(domainNum['Unsubscribe']) + ","
                                numString += str(domainNum['Type']) + ","
                                numString += "https://www.leanplum.com/dashboard?appId=" +  str(app['app_AppID']) + "#/" + str(app['app_AppID']) + "/messaging/" + str(domainNum['MessageID']) + "\n"

                                file.write(numString)
                                break
                    file.close()
                    #Clean up zero records for valid queries (This happens when unique results don't match with subjectResults)
                    file = open(fileName, 'r')
                    lineCount = sum(1 for line in file)
                    if(lineCount == 1):
                        print("\t\tINFO: Zero Records Returned. Deleting Report")
                        os.remove(fileName)
                    else:
                        print("\t\tSuccess")
                    file.close()
                except googleapiclient.errors.HttpError:
                    print("\t\tWarning: This App had bad query. Deleting Report.")
                    file.close()
                    os.remove(fileName)
                    pass
            #attrFile.close()
            print("Finished Running Reports")
#Catch typo from input
else:
    print("\tError: Report type unknown. Please use either \'s\' for Subject Report and \'d\' for Domain Report")



