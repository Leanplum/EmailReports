#This is where we create and manage the push query only

import textwrap

#Main Report
def create_push_notification_query(startDate, endDate, appId, level=0):
	query = """--PIVOT
SELECT
	MessageInfo.MessageId as MessageId,
	SUM(IF(MessageInfo.Event = "Sent", MessageInfo.Occurrence,0)) as Sent,
	SUM(IF(MessageInfo.Event = "Bounce", MessageInfo.Occurrence,0)) as Bounce,
	SUM(IF(MessageInfo.Event = "Held Back", MessageInfo.Occurrence,0)) as Held_Back,
	SUM(IF(MessageInfo.Event = "Open", MessageInfo.Occurrence,0)) as Open
FROM
	(""" + textwrap.indent(join_message_with_event(startDate,endDate,appId,level+1), '\t' * level) + """) MessageInfo
GROUP BY MessageId
Order By MessageId"""
	return query

#Generic Helpers
##We Have this one so we don't mess with grouping
def create_push_message_id_with_name_query(startDate, endDate, appId, level=0):
	query = """--GRAB MessageId's that are Push Notifications along with Name
SELECT
	name as Name,
	__key__.id as MessageId
FROM 
	(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """')))
WHERE (action_type = "__Push Notification" AND app.id = """ + appId + """)
GROUP BY Name, MessageId"""
	return query

def create_push_message_id_query(startDate, endDate, appId, level=0):
# 	query = """--GRAB MessageId's that are Push Notifications
# SELECT
# 	__key__.id as MessageId
# FROM
# 	(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
# 		TIMESTAMP('""" + startDate + """'),
# 		TIMESTAMP('""" + endDate + """')))
# WHERE (action_type = "__Push Notification" AND app.id = """ + appId + """)
# GROUP BY MessageId"""
	query = """--GRAB MessageId's that are Email from Datastore Import
SELECT *
FROM [leanplum-staging:email_report_backups.Push_Message_Ids_""" + str(appId) + "]"
	return query

#Basic Queries
def create_message_events_push_query(startDate, endDate, appId, level=0):
	query = """--Get All Messages and their events
SELECT
	INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
	SUBSTR(states.events.name, 20) AS Event,
	COUNT(*) AS Occurrence
FROM
	(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """')))
WHERE states.events.name LIKE ".m%"
GROUP BY MessageId, Event"""
	return query

def join_message_with_event(startDate, endDate, appId, level=0):
	query = """--Filter out MessageId's Non Push
SELECT
	Session.MessageId as MessageId,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
	END AS Event,
	Session.Occurrence AS Occurrence
FROM
	(""" + textwrap.indent(create_message_events_push_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Session
JOIN
	(""" + textwrap.indent(create_push_message_id_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId"""
	return query
