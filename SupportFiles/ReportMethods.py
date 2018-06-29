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
from SupportFiles import ReportWriter

#Real strong use of dem' globals yo *sigh
Writer = ReportWriter.Writer()
WriterType = ReportWriter.WriterType

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
			Writer.send("Loading Model : " + model + "_backup - " + date, WriterType.INFO)
			job = client.wait_for_job(job_id, timeout=120)
			Writer.send("Model Loaded : " + model + "_backup - " + date, WriterType.INFO)
		#else:
			#Writer.send("Model : " + model + "_backup - " + date + " Exists", flush=True)

def remove_multi_table(client, dateStart, dateEnd, dataset):
	startDate = datetime.datetime.strptime(str(dateStart), '%Y%m%d')
	endDate = datetime.datetime.strptime(str(dateEnd), '%Y%m%d')
	date_generated = [startDate + datetime.timedelta(days=x) for x in range(0, (endDate - startDate + datetime.timedelta(days=1)).days)]

	for date in date_generated:
		Writer.send("Deleting Tables for : " + date.strftime('%Y%m%d'),WriterType.INFO)
		try:
			remove_table(client,date.strftime('%Y%m%d'),dataset)
		except:
			Writer.send("Error Removing Tables",WriterType.DEBUG)
			return
			
def remove_table(client, date, dataset):
	for table_name in ["Study_","App_","Experiment_"]:
		table_name += date
		if( client.check_table(dataset=dataset, table=table_name) ):
			removing = client.delete_table(dataset=dataset,table=table_name)
			if(removing != True):
				Writer.send("Could not delete table :: " + removing,WriterType.DEBUG)

def load_message_ids(client, dataset, appId,messageType='e'):

	appId = int(appId)

	ds_client = datastore.Client(project='leanplum')

	query = ds_client.query(kind='Study')
	key = ds_client.key('App',appId)
	query.add_filter('app','=',key)
	Writer.send("\t\tChecking MessageList for '" + ('email' if messageType == 'e' else 'push') + "'" ,WriterType.INFO)
	if(messageType == 'e'):
		query.add_filter('action_type','=','__Email')
	elif(messageType == 'p'):
		query.add_filter('action_type','=','__Push Notification')

	messageList = list(query.fetch())
	emailList = list(map(lambda entity: entity.key.id, messageList))

	Writer.send("\t\tMessageId's found : " + str(len(emailList)), WriterType.INFO )
	messageIdQuery = create_message_id_list_query(emailList)

	#Try and delete if previous exists
	creation = False
	if(messageType == 'e'):
		delete_generic_table(client=client, table="Email_Message_Ids_" + str(appId),dataset=dataset)
		creation = client.create_table(
			dataset=dataset,
			table= "Email_Message_Ids_" + str(appId),
			schema={"name":"MessageId","type":"integer","mode":"nullable"}
			)
	elif(messageType == 'p'):
		delete_generic_table(client=client, table="Push_Message_Ids_" + str(appId), dataset=dataset)
		creation = client.create_table(
			dataset=dataset,
			table="Push_Message_Ids_" + str(appId),
			schema={"name":"MessageId","type":"integer","mode":"nullable"}
			)
	if (creation == True) :
		# try:
		if(messageType == 'e'):
			messageIdJob = client.write_to_table(
				query=messageIdQuery,
				dataset=dataset,
				table="Email_Message_Ids_" + str(appId),
				use_legacy_sql=False,
				)
		elif(messageType == 'p'):
			messageIdJob = client.write_to_table(
				query=messageIdQuery,
				dataset=dataset,
				table="Push_Message_Ids_" + str(appId),
				use_legacy_sql=False,
				)
		messageIdResults = client.wait_for_job(messageIdJob)
	else:
		Writer.send("Error Creating Table :: " + creation, WriterType.DEBUG )

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