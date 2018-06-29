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

#Real strong use of dem' globals yo *sigh
WriterType = ReportWriter.WriterType

#When a query is executed this method will either use the standard timeout or wait indefinately tell a success response
def wait_for_job(bq_client, query, job, timeOuts,Writer):
	Writer.send(query,WriterType.QUERYWRITER)

	ds_client = datastore.Client(project='leanplum')
	
	if(timeOuts):
		bq_client.wait_for_job(job[0],timeout=240)
	else:
		while(not bq_client.check_job(job[0])[0]):
			time.sleep(5)

def runPushReport(bq_client,reportId, startDate, endDate,timeOuts,Writer):
	Writer.send("\tCreating Report for Push Notifications",WriterType.INFO)

	ds_client = datastore.Client(project='leanplum')

	appResults = []

	#Use the prefix '__A' to indicate this is an AppId not a Company Id
	if(reportId[0:3] == '__A'):
		#Grab the App name and mimic a BQ result
		appNameQuery = ds_client.query(kind='App')
		appKey = ds_client.key('App',int(reportId[3:]))
		appNameQuery.key_filter(appKey,'=')
		appList = list(appNameQuery.fetch())
		try:
			appName = appList[0]['name']
		except KeyError:
			Writer.send("\tWarning: No App Entity for AppId",WriterType.DEBUG)
			return
		appResults = [{'AppName':str(appName),'AppId':str(reportId[3:])}]
	else:
		#Lookup App Id's for the company
		appidsQuery = ReportMethods.create_appids_query(reportId, endDate)
		appJob = bq_client.query(appidsQuery)
		wait_for_job(bq_client, appidsQuery, appJob, timeOuts)
		appResults = bq_client.get_query_rows(appJob[0])

	#Loop through all App's gathered
	for app in appResults:
		#In case the query fails because of missing data or a test app
		try:
			Writer.send("\n\tRunning Report on App :: " + str(app['AppName']) + ":" + str(app['AppId']),WriterType.INFO)

			#Open and create our Report File
			fileName = "./Reports/PushData_" + str(app['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + ".csv"
			directory = os.path.dirname(fileName)
			if not os.path.exists(directory):
				os.makedirs(directory)
			file = open(fileName, "wb")
			file.write("MessageName,StartDate,Sent,Open,Open_PCT,Held Back,Bounce,Bounce_PCT,MessageLink\n".encode('utf-8'))

			#The Study backup wasn't reliable for grabing MessageId's so instead we query the datastore and save the table in BQ
			ReportMethods.load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(app['AppId']),messageType='p')

			###### QUERY BLOCK
			pushQuery = PushGenerator.create_push_notification_query(startDate, endDate, str(app['AppId']))
			pushJob = bq_client.query(pushQuery)
			Writer.send("\t\tRunning Query for Push", WriterType.INFO)
			wait_for_job(bq_client, pushQuery, pushJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			pushResults = bq_client.get_query_rows(pushJob[0])

			pushNameQuery = PushGenerator.create_push_message_id_with_name_query(startDate, endDate, str(app['AppId']))
			pushNameJob = bq_client.query(pushNameQuery)
			Writer.send("\t\tRunning Query for Push Names", WriterType.INFO)
			wait_for_job(bq_client, pushNameQuery, pushNameJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			pushNameResults = bq_client.get_query_rows(pushNameJob[0])
			###### QUERY BLOCK

			#Loop through results and build report
			for pushRows in pushResults:
				for pushName in pushNameResults:
					#Match our MessageId's with the Message Name's
					if pushRows['MessageId'] == pushName['MessageId']:
						#If there are no sents get rid of row
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

							#If there was an error in the above we catch that here and continue
							if(messageStartDate == ""):
								Writer.send("\t\t\tDataStore has no record of MessageId STUDY:: " + str(pushRows['MessageId']),WriterType.DEBUG )
								messageStartDate = "Unknown"


							openPct = float(pushRows['Open'])/float(pushRows['Sent']) * 100.0
							bouncePCT = float(pushRows['Bounce'])/float(pushRows['Sent']) * 100.0

							numString = ""

							#Redundancy
							if(datastorePushName == ""):
								numString += "\"" + str(pushName['Name']) + "\","
							else:
								numString += "\"" + str(datastorePushName) + "\","

							#Write values to file
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
			#Clean up zero records for valid queries. This usually happens when Unique results don't match with regular results
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
		#The Table we wrote to BQ gets erased here
		Writer.send("\t\tCleaning up dataset . . ",WriterType.INFO,ending="")
		ReportMethods.delete_generic_table(client=bq_client, table="Push_Message_Ids_" + str(app['AppId']), dataset='email_report_backups')
		Writer.send("Clean",WriterType.INFO)

	Writer.send("Finished Running Reports",WriterType.INFO)