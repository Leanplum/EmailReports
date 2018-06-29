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
import time
import sys
from SupportFiles import DomainLineQueryGen as DomainGenerator
from SupportFiles import SubjectLineQueryGen as SubjectGenerator
from SupportFiles import PushNotificationQueryGen as PushGenerator
from SupportFiles import ReportMethods
from SupportFiles import SubjectLineReport as SubjectReport
from SupportFiles import DomainLineReport as DomainReport
from SupportFiles import PushLineReport as PushReport
from SupportFiles import ReportWriter

Writer = ReportWriter.Writer()

def updateWriter(debug):
    Writer.info = (debug & 1)
    Writer.debug = (debug & 2)
    Writer.querywriter = (debug & 4)

def runReport(reportId, startDate, endDate, reportType,timeOuts=0,debug=0):

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

    # Update Writer
    updateWriter(debug)

    # initialize google and bq clients
    google_credential = GoogleCredentials.get_application_default()
    google_service = googleapiclient.discovery.build('storage', 'v1', credentials=google_credential)
    bq_client = bigquery.get_client(project_id='leanplum-staging', credentials=google_credential)

    #Delete backups over time range
    if(reportType == 'delete'):
        print("Removing backups over timerange")
        ReportMethods.remove_multi_table(
            client=bq_client,
            dateStart=startDate,
            dateEnd=endDate,
            dataset='email_report_backups'
            )

        Writer.send("\tBackups removed for time range",WriterType.INFO)
        return

    #Load all the backups
    for model in ['App','Study','Experiment']:
        Writer.send("Loading " + model, WriterType.INFO)
        ReportMethods.load_multi_table(service=google_service,
                   client=bq_client,
                   dateStart=startDate,
                   dateEnd=endDate,
                   bucket='leanplum_backups',
                   dataset='email_report_backups',
                   model=model)

    Writer.send("\tBackups Loaded",WriterType.INFO)

    #Subject Report
    if(reportType[0] == 's'):
        SubjectReport.runSubjectReport(bq_client,reportId, startDate,endDate,timeOuts)
    #Domain Report
    elif(reportType == 'd'):
        DomainReport.runDomainReport(bq_client,reportId,startDate,endDate,timeOuts)
    #Push Report
    elif(reportType == 'p'):
        PushReport.runPushReport(bq_client,reportId,startDate,endDate,timeOuts)