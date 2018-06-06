#This is where we create and manage the domain line query only

#1> Full Domain Report
#	2>Delivery Type
#	2>Pivot Table
#		3> Sum Email Domains together
#			4> Join Domain Events
#				5> All Events 
#				5> All Email Messages

#1> Unique Domain Report (Pivot)
#	2> Join Unique Domain Events
#		3> All Unique Events
#		3> All Email Messages
import textwrap

Domains = "(\"gmail.com\",\"msn.com\",\"hotmail.com\", \"yahoo.com\")"
# MAIN REPORT
def create_domain_line_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--JOIN MessageInfo with DeliveryType
SELECT
	Study.MessageName as MessageName,
	MessageInfo.MessageId as MessageId,
	MessageInfo.Domain as Domain,
	MessageInfo.Sent as Sent,
	MessageInfo.Delivered as Delivered,
	MessageInfo.Open as Open,
	MessageInfo.Click as Click,
	MessageInfo.Bounce as Bounce,
	MessageInfo.Dropped as Dropped,
	MessageInfo.Block as Block,
	MessageInfo.Unsubscribe as Unsubscribe,
	MessageInfo.Spam as Spam,
	ROW_NUMBER() OVER(PARTITION BY MessageInfo.MessageId, MessageInfo.Domain ORDER BY Study.time DESC) AS ID,
	Study.Type as Type
FROM
	(""" + textwrap.indent(pivot_domain_query(startDate, endDate, appId, attrLoc, level + 1),'\t' * level) + """) MessageInfo
JOIN
	(""" + textwrap.indent(create_delivery_type_query(startDate, endDate, appId, attrLoc, level + 1),'\t' * level) + """) Study
ON Study.MessageId = MessageInfo.MessageId
GROUP BY MessageName, MessageInfo.MessageId, MessageId, MessageInfo.Domain, Domain, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe, Spam, Study.time, Type
ORDER BY MessageId, Domain"""
	return query

def create_unique_domain_query(startDate, endDate, appId, attrLoc, level=0):
	#Wrapper
	return pivot_unique_domain_query(startDate, endDate, appId, attrLoc, level)

# GENERIC HELPERS
def create_email_message_id_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--GRAB MessageId's that are Emails
SELECT
	__key__.id as MessageId
FROM
	(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """')))
WHERE (action_type = "__Email" AND app.id = """ + appId + """)
GROUP BY MessageId"""
	return query

def create_delivery_type_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--Delivery Type
SELECT
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
		TIMESTAMP('""" + endDate + """')))"""
	return query

# UNIQUE QUERIES

# THIS IS INTENTIONALLY WRONG. We group on EventTime to mimic bad analytics since we consider uniques per day not uniquers per user.
def create_unique_message_events_domain_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--Unique Events for Messages
SELECT
	user_id,
	INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
	LOWER(attr"""+ attrLoc + """) as Email,
	SUBSTR(states.events.name, 20) AS Event,
	SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS eventtime
FROM
	(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """')))
WHERE states.events.name LIKE ".m%"
GROUP BY user_id, MessageId, Event, Email, eventtime"""
	return query

def join_unique_message_with_events(startDate, endDate, appId, attrLoc, level=0):
	query = """--Join MessageEvents with MessageId's
SELECT
	Session.MessageId AS MessageID,
	CASE
		WHEN (REGEXP_EXTRACT(Session.Email, r'@(.+)') NOT IN """ + Domains + """ OR REGEXP_EXTRACT(Session.Email, r'@(.+)') IS NULL ) THEN "Other" 
		ELSE REGEXP_EXTRACT(Session.Email, r'@(.+)')
		END AS Domain,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Event,
	COUNT(Session.Event) AS Occurrence
FROM
	(""" + textwrap.indent(create_unique_message_events_domain_query(startDate, endDate, appId, attrLoc, level + 1),'\t' * level) + """) Session
JOIN
	(""" + textwrap.indent(create_email_message_id_query(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) Study
ON Study.MessageId = Session.MessageId
GROUP BY MessageId, Domain, Event"""
	return query

def pivot_unique_domain_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--Pivot Uniques
SELECT
	MessageInfo.MessageId as MessageId,
	MessageInfo.Domain as Domain,
	SUM(IF(MessageInfo.Event = "Open", MessageInfo.Occurrence,0)) AS Unique_Open,
	SUM(IF(MessageInfo.Event = "Click", MessageInfo.Occurrence,0)) AS Unique_Click
FROM
	(""" + textwrap.indent(join_unique_message_with_events(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) MessageInfo
GROUP BY MessageId, Domain,
ORDER BY MessageId"""
	return query

# BASIC QUERIES
def pivot_domain_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--PIVOT
SELECT
  	MessageInfo.MessageId as MessageId,
  	MessageInfo.Domain as Domain,
  	SUM(IF(MessageInfo.Event = "Sent", MessageInfo.Occurrence,0)) AS Sent,
  	SUM(IF(MessageInfo.Event = "Delivered", MessageInfo.Occurrence,0)) AS Delivered,
  	SUM(IF(MessageInfo.Event = "Open", MessageInfo.Occurrence,0)) AS Open,
  	SUM(IF(MessageInfo.Event = "Click", MessageInfo.Occurrence,0)) AS Click,
  	SUM(IF(MessageInfo.Event = "Bounce", MessageInfo.Occurrence,0)) AS Bounce,
  	SUM(IF(MessageInfo.Event = "Dropped", MessageInfo.Occurrence,0)) AS Dropped,
  	SUM(IF(MessageInfo.Event = "Block", MessageInfo.Occurrence,0)) AS Block,
  	SUM(IF(MessageInfo.Event = "Unsubscribe", MessageInfo.Occurrence,0)) AS Unsubscribe,
  	SUM(IF(MessageInfo.Event = "Marked as spam", MessageInfo.Occurrence,0)) AS Spam
FROM
	(""" + textwrap.indent(sum_related_domains_query(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) MessageInfo
GROUP BY MessageId, Domain
ORDER BY MessageId"""
	return query

def sum_related_domains_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--Sum Domains
SELECT
	MessageSplit.MessageId as MessageId,
	MessageSplit.Domain as Domain,
	MessageSplit.Event as Event,
	SUM(MessageSplit.Occurrence) as Occurrence
FROM
	(""" + textwrap.indent(join_message_with_event(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) MessageSplit
GROUP BY MessageId, Domain, Event"""
	return query

def join_message_with_event(startDate, endDate, appId, attrLoc, level=0):
	query = """--Join messageEvents on Domain with messageIds
SELECT
	Session.MessageId as MessageID,
	CASE
	  WHEN (REGEXP_EXTRACT(Session.Email, r'@(.+)') NOT IN """ + Domains + """ OR REGEXP_EXTRACT(Session.Email, r'@(.+)') IS NULL ) THEN "Other" 
	  ELSE REGEXP_EXTRACT(Session.Email, r'@(.+)')
	END AS Domain,
	CASE
	  WHEN Session.Event="" THEN "Sent"
	  ELSE Session.Event
	END AS Event,
	Session.Occurrence AS Occurrence
FROM
	(""" + textwrap.indent(create_message_events_domain_query(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) Session
JOIN
	(""" + textwrap.indent(create_email_message_id_query(startDate, endDate, appId, attrLoc, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId"""
	return query

def create_message_events_domain_query(startDate, endDate, appId, attrLoc, level=0):
	query = """--Get All Messages and their email domain
SELECT
	INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
	LOWER(attr"""+ attrLoc + """) as Email,
	SUBSTR(states.events.name, 20) AS Event,
	COUNT(*) as Occurrence
FROM
	(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """')))
WHERE states.events.name LIKE ".m%"
GROUP BY MessageId, Event, Email"""
	return query