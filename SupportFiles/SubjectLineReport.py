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
from SupportFiles import ReportWriter

#Real strong use of dem' globals yo *sigh
WriterType = ReportWriter.WriterType

def wait_for_job(bq_client, query, job, timeOuts,Writer):
	Writer.send(query,WriterType.QUERYWRITER)

	ds_client = datastore.Client(project='leanplum')

	if(timeOuts):
		bq_client.wait_for_job(job[0],timeout=240)
	else:
		while(not bq_client.check_job(job[0])[0]):
			time.sleep(5)

def runSubjectReport(bq_client,reportId,reportType,startDate, endDate,timeOuts,Writer):
	Writer.send('\tCreating report by Subject Line',WriterType.INFO)

	ds_client = datastore.Client(project='leanplum')

	appResults = []

	if(reportId[0:3] == '__A'):
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
		#Create App Id's Query
		appidsQuery = ReportMethods.create_appids_query(reportId, endDate)
		appJob = bq_client.query(appidsQuery)
		wait_for_job(bq_client, appidsQuery, appJob, timeOuts, Writer)
		appResults = bq_client.get_query_rows(appJob[0])

	#Loop through all App Id's
	for appBundle in appResults:
		Writer.send("\n\tRunning Report on App :: " + appBundle['AppName'] + ":" + str(appBundle['AppId']), WriterType.INFO)

		ReportMethods.load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(appBundle['AppId']), messageType = 'e')

		ds_client = datastore.Client(project='leanplum')
		try:
			Writer.send("\t\tTapping Datastore for App Categories",WriterType.INFO)
			appQuery = ds_client.query(kind='App')
			appKey = ds_client.key('App',int(appBundle['AppId']))
			appQuery.key_filter(appKey,'=')
			appList = list(appQuery.fetch())
			categoryList = appList[0]['unsubscribe_categories']
		except KeyError:
			Writer.send("\t\tINFO: No UnsubscribeCategories",WriterType.INFO)
			categoryList = []
		except IndexError:
			Writer.send("\t\tWARNING: Could not find AppId in datastore",WriterType.INFO)
			categoryList = []
		#In case the query fails because of missing data or a test app
		try:
			fileName = "./Reports/EmailData_" + str(appBundle['AppName']).replace("/","-") + "_" + str(startDate) + "_" + str(endDate) + "_subject.csv"
			directory = os.path.dirname(fileName)
			if not os.path.exists(directory):
				os.makedirs(directory)
			file = open(fileName, "wb")
			file.write("Subject,StartDate,Sent,Delivered,Delivered_PCT,Open,Open_PCT,Unique_Open,Unique_Open_PCT,Unique_Click,Unique_Click_PCT,Bounce,Bounce_PCT,Dropped,Unsubscribe,Spam,Spam_PCT,Category,MessageLink\n".encode('utf-8'))

			###### QUERY BLOCK
			subjectLineQuery = SubjectGenerator.create_subject_line_query(startDate, endDate, str(appBundle['AppId']))
			Writer.send("\t\tRunning Query", WriterType.INFO)
			subjectLineJob = bq_client.query(subjectLineQuery)
			wait_for_job(bq_client, subjectLineQuery, subjectLineJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			subjectResults = bq_client.get_query_rows(subjectLineJob[0])

			uniqLineQuery = SubjectGenerator.create_unique_line_query(startDate, endDate, str(appBundle['AppId']))
			Writer.send("\t\tRunning Query for Uniques", WriterType.INFO)
			uniqLineJob = bq_client.query(uniqLineQuery)
			wait_for_job(bq_client, uniqLineQuery, uniqLineJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			uniqResults = bq_client.get_query_rows(uniqLineJob[0])
			###### QUERY BLOCK

			#There is a difference between a bad table and a zero table. We catch that here.
			if(not subjectResults):
				Writer.send("\t\tINFO: Zero Records Returned", WriterType.INFO)
				file.close()
				os.remove(fileName)
				continue

			#Check if we are running AB Reports before we spend the cash money
			if( reportType[1] == "1" ):

				###### QUERY BLOCK
				Writer.send("\t\t----AB Query On----",WriterType.INFO)
				abQuery = SubjectGenerator.create_ab_query(startDate, endDate, str(appBundle['AppId']))
				Writer.send("\t\tRunning AB Query", WriterType.INFO)
				abJob = bq_client.query(abQuery)
				wait_for_job(bq_client, abQuery, abJob, timeOuts, Writer)
				Writer.send("\t\tQuery Success", WriterType.INFO)
				abResults = bq_client.get_query_rows(abJob[0])
				Writer.send("\t\t\t" + str(len(abResults)) + " Variants Found",WriterType.INFO)

				abUniqueQuery = SubjectGenerator.create_unique_ab_query(startDate, endDate, str(appBundle['AppId']))
				Writer.send("\t\tRunning AB Unique Query", WriterType.INFO)
				abUniqueJob = bq_client.query(abUniqueQuery)
				wait_for_job(bq_client, abUniqueQuery, abUniqueJob, timeOuts, Writer)
				Writer.send("\t\tQuery Success", WriterType.INFO)
				abUniqueResults = bq_client.get_query_rows(abUniqueJob[0])
				Writer.send("\t\t\t" + str(len(abUniqueResults)) + " Unique Variants Found",WriterType.INFO)

				variantSLQuery = SubjectGenerator.variant_subject_line_query(startDate, endDate, str(appBundle['AppId']))
				Writer.send("\t\tRunning AB Subject Line Query", WriterType.INFO)
				variantSLJob = bq_client.query(variantSLQuery)
				wait_for_job(bq_client, variantSLQuery, variantSLJob, timeOuts, Writer)
				Writer.send("\t\tQuery Success",WriterType.INFO)
				variantSLResults = bq_client.get_query_rows(variantSLJob[0])
				Writer.send("\t\t\t" + str(len(variantSLResults)) + " Variant Subject Lines Found", WriterType.INFO)
				###### QUERY BLOCK

			#Loop through all the MessageId's that we gathered from the AppId
			for item in subjectResults:
				for uni in uniqResults:
					if(uni['MessageId'] == item['MessageId']):
						if(int(item['Sent'] == 0)):
							Writer.send("\t\tINFO: Skipping MessageId :: " + str(item['MessageId']) + " :: due to no `Sent` events for time range",WriterType.DEBUG)
							break

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
								Writer.send("\t\tWarning: Captured wrong message :: " + str(item['MessageId']) + " :: Type = " + str(qList[0]['action_type']), WriterType.DEBUG)
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
							Writer.send("\t\t\tDataStore has no record of MessageId STUDY:: " + str(item['MessageId']),WriterType.DEBUG )
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
				Writer.send("\t\tINFO: Error reading end file",WriterType.DEBUG)
			else:
				lineCount = int(result.strip().split()[0])

			if(lineCount == 1):
				Writer.send("\t\tINFO: Zero Records Returned. Deleting Report.",WriterType.INFO)
				os.remove(fileName)
			else:
				Writer.send("\t\tSuccess",WriterType.INFO)
			file.close()
		
		except googleapiclient.errors.HttpError as inst:
			Writer.send("\t\tWarning: This App had bad query. Deleting Report. " + str(type(inst)),WriterType.DEBUG)
			file.close()
			os.remove(fileName)
			pass
		Writer.send("\t\tCleaning up dataset . . ",WriterType.INFO,ending="")
		ReportMethods.delete_generic_table(client=bq_client, table="Email_Message_Ids_" + str(appBundle['AppId']), dataset='email_report_backups')
		Writer.send("Clean",WriterType.INFO)

	Writer.send("Finished Running Reports",WriterType.INFO)