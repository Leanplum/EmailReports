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

Domains = "(\"gmail.com\",\"msn.com\",\"hotmail.com\", \"yahoo.com\")"
# MAIN REPORT
def create_domain_line_query(startDate, endDate, appId, attrLoc):
	query = """
	--JOIN MessageInfo with DeliveryType
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
		ROW_NUMBER() OVER(PARTITION BY MessageInfo.MessageId, MessageInfo.Domain ORDER BY Study.time DESC) AS ID,
		Study.Type as Type
	FROM
		(""" + pivot_domain_query(startDate, endDate, appId, attrLoc) + """) MessageInfo
	JOIN
		(""" + create_delivery_type_query(startDate, endDate, appId, attrLoc) + """) Study
	ON Study.MessageId = MessageInfo.MessageId
	GROUP BY MessageName, MessageInfo.MessageId, MessageId, MessageInfo.Domain, Domain, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe, Study.time, Type
	ORDER BY MessageId, Domain
	"""
	return query

def create_unique_domain_query(startDate, endDate, appId, attrLoc):
	#Wrapper
	return pivot_unique_domain_query(startDate, endDate, appid, attrLoc)

# GENERIC HELPERS
def create_email_message_id_query(startDate, endDate, appId, attrLoc):
	query = """
	--GRAB MessageId's that are Emails
	SELECT
		__key__.id as MessageId
	FROM
		(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Study_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """')))
	WHERE (action_type = "__Email" AND app.id = """ + appId + """)
	GROUP BY MessageId
	"""
	return query

def create_delivery_type_query(startDate, endDate, appId, attrLoc):
	query = """
	--Delivery Type
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
			TIMESTAMP('""" + endDate + """')))
	"""
	return query

# UNIQUE QUERIES

# THIS IS INTENTIONALLY WRONG. We group on EventTime to mimic bad analytics since we consider uniques per day not uniquers per user.
def create_unique_message_events_domain_query(startDate, endDate, appId, attrLoc):
	query = """
	--Unique Events for Messages
	SELECT
		user_id,
		INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
		attr"""+ attrLoc + """ as Email,
		SUBSTR(states.events.name, 20) AS Event,
		SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS eventtime
	FROM
		(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """')))
	WHERE states.events.name LIKE ".m%"
	GROUP BY user_id, MessageId, Event, Email, eventtime
	"""
	return query

def join_unique_message_with_events(startDate, endDate, appId, attrLoc):
	query = """
	--Join MessageEvents with MessageId's
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
		(""" +create_unique_message_events_domain_query(startDate, endDate, appId, attrLoc) + """) Session
	JOIN
		(""" + create_email_message_id_query(startDate, endDate, appId, attrLoc) + """) Study
	ON Study.MessageId = Session.MessageId
	GROUP BY MessageId, Domain, Event
	"""
	return query

def pivot_unique_domain_query(startDate, endDate, appId, attrLoc):
	query = """
	--Pivot Uniques
	SELECT
		MessageInfo.MessageId as MessageId,
		MessageInfo.Domain as Domain,
		SUM(IF(MessageInfo.Event = "Open", MessageInfo.Occurrence,0)) AS Unique_Open,
		SUM(IF(MessageInfo.Event = "Click", MessageInfo.Occurrence,0)) AS Unique_Click
	FROM
		(""" + join_unique_message_with_events(startDate, endDate, appId, attrLoc) + """) MessageInfo
	GROUP BY MessageId, Domain,
	ORDER BY MessageId
	"""
	return query

# BASIC QUERIES
def pivot_domain_query(startDate, endDate, appId, attrLoc):
	query = """
	--PIVOT
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
	  SUM(IF(MessageInfo.Event = "Unsubscribe", MessageInfo.Occurrence,0)) AS Unsubscribe
	FROM
		(""" + sum_related_domains_query(startDate, endDate, appId, attrLoc) + """) MessageInfo
	GROUP BY MessageId, Domain
	ORDER BY MessageId
	"""
	return query

def sum_related_domains_query(startDate, endDate, appId, attrLoc):
	query = """
	--Sum Domains
	SELECT
		MessageSplit.MessageId as MessageId,
		MessageSplit.Domain as Domain,
		MessageSplit.Event as Event,
		SUM(MessageSplit.Occurrence) as Occurrence
	FROM
		(""" + join_message_with_event(startDate, endDate, appId, attrLoc) + """) MessageSplit
	GROUP BY MessageId, Domain, Event
	"""
	return query

def join_message_with_event(startDate, endDate, appId, attrLoc):
	query = """
	--Join messageEvents on Domain with messageIds
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
		(""" + create_message_events_domain_query(startDate, endDate, appId, attrLoc) + """) Session
	JOIN
		(""" + create_email_message_id_query(startDate, endDate, appId, attrLoc) + """) Study
	ON Session.MessageId = Study.MessageId
	"""
	return query

def create_message_events_domain_query(startDate, endDate, appId, attrLoc):
	query = """
	--Get All Messages and their email domain
	SELECT
		INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
		attr"""+ attrLoc + """ as Email,
		SUBSTR(states.events.name, 20) AS Event,
		COUNT(*) as Occurrence
	FROM
		(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """')))
	WHERE states.events.name LIKE ".m%"
	GROUP BY MessageId, Event, Email
	"""
	return query