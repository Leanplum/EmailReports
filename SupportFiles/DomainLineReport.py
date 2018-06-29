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

def wait_for_job(bq_client, query, job, timeOuts, Writer):
	Writer.send(query,WriterType.QUERYWRITER)

	ds_client = datastore.Client(project='leanplum')

	if(timeOuts):
		bq_client.wait_for_job(job[0],timeout=240)
	else:
		while(not bq_client.check_job(job[0])[0]):
			time.sleep(5)

def runDomainReport(bq_client,reportId,startDate,endDate,timeOuts, Writer):
	Writer.send('\tCreating report by Domain',WriterType.INFO)

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
		#Lookup App Id's for the company
		appidsQuery = ReportMethods.create_appids_query(reportId, endDate)
		appJob = bq_client.query(appidsQuery)
		wait_for_job(bq_client, appidsQuery, appJob, timeOuts, Writer)
		appResults = bq_client.get_query_rows(appJob[0])

	#Loop through all App's gathered
	for app in appResults:

		#In case the query fails because of missing data or a test app
		try:
			Writer.send("\n\tRunning Report on App :: " + str(app['AppName']) + ":" + str(app['AppId']),WriterType.INFO)

			ReportMethods.load_message_ids(client=bq_client, dataset='email_report_backups', appId=str(app['AppId']))

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
			Writer.send("\t\tTapping Datstore:App", WriterType.INFO)
			appList = list(query.fetch())

			try:
			#Should only return the AppData for appId specific in key
				if(len(appList)!= 1):
					Writer.send('\t\tBad App Entities returned from AppID for ' + str(app['AppName']) + '.Ignore for Unwanted Apps',WriterType.DEBUG)
				else:
					emailName = dict(appList[0])['email_user_attribute']
					#Run query against app data to find location of email attr
					query = ds_client.query(kind='AppData')
					key = ds_client.key('AppData',appId)
					query.key_filter(key,'=')

					Writer.send("\t\tTapping Datastore:AppData", WriterType.INFO)
					appDataList = list(query.fetch())
					try:
						categoryList = appList[0]['unsubscribe_categories']
					except KeyError:
						Writer.send("\t\tINFO: App has no email categories", WriterType.INFO)

					if(len(appDataList) != 1):
						Writer.send('\t\tBad AppData Entities returned from AppID for ' + str(app['AppName']) + '.Ignore for Unwanted Apps',WriterType.DEBUG)
					else:
						#Count rows to find email location
						attrColumns = dict(appDataList[0])['attribute_columns']
						for attr in attrColumns:
							if(attr == emailName):
								break
							else:
								emailLoc = emailLoc + 1

			except KeyError:
				Writer.send("\t\tWarning: This App doesn't have email attribute specified on App *OR* in user attributes list.",WriterType.DEBUG)
				pass
			#Set emailLocation to string - Lazy
			attrLoc = str(emailLoc)
			Writer.send('\t\tEmail Name : ' + emailName + ' : at Location : ' + attrLoc, WriterType.INFO)

			###### QUERY BLOCK
			domainQuery = DomainGenerator.create_domain_line_query(startDate, endDate, str(app['AppId']), attrLoc)
			Writer.send("\t\tRunning Query for Domain", WriterType.INFO)
			domainJob = bq_client.query(domainQuery)
			wait_for_job(bq_client, domainQuery, domainJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			domainResults = bq_client.get_query_rows(domainJob[0])

			domainUniqueQuery = DomainGenerator.create_unique_domain_query(startDate, endDate, str(app['AppId']), attrLoc)
			Writer.send("\t\tRunning Query for Uniques", WriterType.INFO)
			domainUniJob = bq_client.query(domainUniqueQuery)
			wait_for_job(bq_client, domainUniqueQuery, domainUniJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			domainUniResults = bq_client.get_query_rows(domainUniJob[0])

			senderEmailQuery = ReportMethods.create_sender_email_query(startDate, endDate)
			Writer.send("\t\tRunning Query for Sender Emails", WriterType.INFO)
			senderJob = bq_client.query(senderEmailQuery)
			wait_for_job(bq_client, senderEmailQuery, senderJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			senderEmailResults = bq_client.get_query_rows(senderJob[0])

			defaultEmailSenderQuery = ReportMethods.create_default_sender_email_query(str(app['AppId']), str(endDate))
			Writer.send("\t\tRunning Query for Default Sender Email", WriterType.INFO)
			defaultEmailJob = bq_client.query(defaultEmailSenderQuery)
			wait_for_job(bq_client, defaultEmailSenderQuery, defaultEmailJob, timeOuts, Writer)
			Writer.send("\t\tQuery Success", WriterType.INFO)
			defaultEmail = bq_client.get_query_rows(defaultEmailJob[0])[0]['email_from_address']
			###### QUERY BLOCK

			#Used for All Category -- keep running track of value for messageId
			allCategoryDict = {'MessageName':'','MessageId':0,'SenderDomain':'','Domain':'All','Sent':0,'Delivered':0,'Open':0,'Unique_Open':0,'Unique_Click':0,'Bounce':0,'Dropped':0,'Unsubscribe':0,'Spam':0,'Type':'','Category':'Default','MessageLink':''}

			#Loop through all results and build report
			for domainNum in domainResults:
				for domainUni in domainUniResults:
					if(str(domainNum['Domain']) == str(domainUni['Domain']) and str(domainNum['MessageId']) == str(domainUni['MessageId'])):
						if(int(domainNum['Sent']) == 0 or int(domainNum['ID']) != 1):
							if(int(domainNum['Sent']) == 0):
								Writer.send("\t\tINFO: Skipping MessageId :: " + str(domainNum['MessageId']) + " :: on domain :: " + str(domainNum['Domain']) + " :: due to no `Sent` events for time range",WriterType.DEBUG)
							elif(int(domainNum['ID']) != 1):
								Writer.send("\t\tINFO: Skipping Duplicate Message Row :: " + str(domainNum['MessageId']) + " :: due to multiple ID's availble. --> ID's == " + str(domainNum['ID']),WriterType.DEBUG)
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
									Writer.send("\t\tWarning: Captured wrong message :: " + str(domainNum['MessageId']) + " :: Type = " + str(qList[0]['action_type']), WriterType.DEBUG)
									break

								messageStartDate = str(qList[0]['active_since'].date())
								try:
									categoryId = int(qList[0]['category_id'])
									for categoryDict in categoryList:
										if(categoryId == int(categoryDict['id'])):
											categoryName = str(categoryDict['name'])
											allCategoryDict['Category'] = categoryName
								except TypeError:
									#message is in the default category

									pass
							except IndexError:
								pass
							except AttributeError:
								pass
							if(messageStartDate == ""):
								Writer.send("\t\t\tDataStore has no record of MessageId STUDY:: " + str(domainNum['MessageId']),WriterType.DEBUG )
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
									Writer.send("\t\tINFO: Not Writing `All` Row for due to no no `Sends` for MessageId :: " + str(allCategoryDict['MessageId']),WriterType.DEBUG)

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
								Writer.send("\t\tWarning: Captured wrong message :: " + str(domainNum['MessageId']) + " :: Type = " + str(qList[0]['action_type']), WriterType.DEBUG)
								break
							messageStartDate = str(qList[0]['active_since'].date())
							try:
								categoryId = int(qList[0]['category_id'])
								for categoryDict in categoryList:
									if(categoryId == int(categoryDict['id'])):
										categoryName = str(categoryDict['name'])
									break
							except TypeError:
								#Message is in Default Category
								pass
						except IndexError:
							pass
						except AttributeError:
							pass
						if(messageStartDate == ""):
							Writer.send("\t\t\tDataStore has no record of MessageId STUDY:: " + str(domainNum['MessageId']),WriterType.DEBUG )
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
				Writer.send("\t\tINFO: Error reading end file",WriterType.DEBUG)
			else:
				lineCount = int(result.strip().split()[0])

			if(lineCount == 1):
				Writer.send("\t\tINFO: Zero Records Returned. Deleting Report",WriterType.INFO)
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
		ReportMethods.delete_generic_table(client=bq_client, table="Email_Message_Ids_" + str(app['AppId']), dataset='email_report_backups')
		Writer.send("Clean",WriterType.INFO)

	Writer.send("Finished Running Reports",WriterType.INFO)