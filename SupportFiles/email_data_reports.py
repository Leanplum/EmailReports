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

WriterType = ReportWriter.WriterType

def updateWriter(Writer,debug):
    Writer.info = (debug & 1)
    Writer.debug = (debug & 2)
    Writer.querywriter = (debug & 4)

def runReport(reportId, startDate, endDate, reportType,timeOuts=0,debug=0):

    # Update Writer

    Writer = ReportWriter.Writer()
    updateWriter(Writer,debug)

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
        SubjectReport.runSubjectReport(bq_client,reportId,reportType,startDate,endDate,timeOuts,Writer)
    #Domain Report
    elif(reportType == 'd'):
        DomainReport.runDomainReport(bq_client,reportId,startDate,endDate,timeOuts,Writer)
    #Push Report
    elif(reportType == 'p'):
        PushReport.runPushReport(bq_client,reportId,startDate,endDate,timeOuts,Writer)