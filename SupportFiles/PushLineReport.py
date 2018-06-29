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
from SupportFiles import ReportWriter

def runPushReport(bq_client,companyId, startDate, endDate,timeOuts):
	#Lookup App Id's for the company
	appidsQuery = ReportMethods.create_appids_query(companyId, endDate)
	appJob = bq_client.query(appidsQuery)
	if(timeOuts):
		bq_client.wait_for_job(appJob[0],timeout=120)
	else:
		while(not bq_client.check_job(appJob[0])[0]):
			time.sleep(5)
	appResults = bq_client.get_query_rows(appJob[0])
	#Loop through all App's gathered
	for app in appResults:
		#In case the query fails because of missing data or a test app
		try:
			Writer.send("\n\tRunning Report on App :: " + str(app['AppName']) + ":" + str(app['AppId']),WriterType.INFO)

			fileName = "./Reports/PushData_" + str(app['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + ".csv"
			directory = os.path.dirname(fileName)
			if not os.path.exists(directory):
				os.makedirs(directory)
			file = open(fileName, "wb")
			file.write("MessageName,StartDate,Sent,Open,Open_PCT,Held Back,Bounce,Bounce_PCT,MessageLink\n".encode('utf-8'))

			ReportMethods.load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(app['AppId']),messageType='p')

			pushQuery = PushGenerator.create_push_notification_query(startDate, endDate, str(app['AppId']))
			Writer.send(pushQuery,WriterType.QUERYWRITER)
			pushJob = bq_client.query(pushQuery)
			Writer.send("\t\tRunning Query for Push", WriterType.INFO)
			if(timeOuts):
				bq_client.wait_for_job(pushJob[0],timeout=120)
			else:
				while(not bq_client.check_job(pushJob[0])[0]):
					time.sleep(5)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			pushResults = bq_client.get_query_rows(pushJob[0])

			pushNameQuery = PushGenerator.create_push_message_id_with_name_query(startDate, endDate, str(app['AppId']))
			Writer.send(pushNameQuery,WriterType.QUERYWRITER)
			pushNameJob = bq_client.query(pushNameQuery)
			Writer.send("\t\tRunning Query for Push Names", WriterType.INFO)
			if(timeOuts):
				bq_client.wait_for_job(pushNameJob[0],timeout=120)
			else:
				while(not bq_client.check_job(pushNameJob[0])[0]):
					time.sleep(5)
			Writer.send("\t\tQuery Success", WriterType.INFO)
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
									Writer.send("\t\tWarning: Captured wrong message :: " + str(pushRows['MessageId']) + " :: Type = " + str(qList[0]['action_type']), WriterType.DEBUG)
									break
								datastorePushName = str(qList[0]['name'])
								messageStartDate = str(qList[0]['active_since'].date())
							except IndexError:
								pass
							except AttributeError:
								pass

							if(messageStartDate == ""):
								Writer.send("\t\t\tDataStore has no record of MessageId STUDY:: " + str(pushRows['MessageId']),WriterType.DEBUG )
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
				Writer.send("\t\tINFO: Error reading end file", WriterType.INFO)
			else:
				lineCount = int(result.strip().split()[0])

			if(lineCount == 1):
				Writer.send("\t\tINFO: Zero Records Returned. Deleting Report",WriterType.INFO)
				os.remove(fileName)
			else:
				Writer.send("\t\tSuccess",WriterType.INFO)
			file.close()
		except googleapiclient.errors.HttpError as inst:
			Writer.send("\t\tWarning: This App had a bad query. Deleting Report. " + str(type(inst)),WriterType.INFO)
			file.close()
			os.remove(fileName)
			pass
		Writer.send("\t\tCleaning up dataset . . ",ending="",WriterType.INFO)
		ReportMethods.delete_generic_table(client=bq_client, table="Push_Message_Ids_" + str(app['AppId']), dataset='email_report_backups')
		Writer.send("Clean",WriterType.INFO)

	Writer.send("Finished Running Reports",WriterType.INFO)