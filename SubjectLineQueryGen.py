#This is where we create and manage the subject line query only

# 1> Full Subject Report
#               2> Pivot Subject Query
#                           3> Join Event Message Query
#                                   4> Event Message Query
#                                   4> Message ID Query
#               2> Subject Line Query

# 1> Unique Subject Report
#				2> Pivot Unique Query
#							3> Join Unique Message Query
#									4>Unique Event Message Query
#									4> Message ID Query



# MAIN REPORT 
def create_subject_line_query(startDate, endDate, appId):
	query = """
	--JOIN Message Counts with Subject Line
	SELECT
		Subject.SubjectLine as Subject,
		Subject.MessageId as MessageId,
		MessageInfo.Sent as Sent,
		MessageInfo.Delivered as Delivered,
		MessageInfo.Open as Open,
		MessageInfo.Click as Click,
		MessageInfo.Bounce as Bounce,
		MessageInfo.Dropped as Dropped,
		MessageInfo.Block as Block,
		MessageInfo.Unsubscribe as Unsubscribe
	FROM
		(""" + pivot_subject_query(startDate, endDate, appId) + """) MessageInfo
	JOIN
		(""" + subject_line_query(startDate, endDate, appId) + """) Subject
	ON Subject.MessageId = MessageInfo.MessageId
	GROUP BY Subject, MessageId, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe
	ORDER BY MessageId
	"""
	return query

def create_unique_line_query(startDate, endDate, appId):
	#Wrapper
	return pivot_unique_subject_query(startDate, endDate, appId)

# GENERIC HELPERS 
def subject_line_query(startDate, endDate, appId):
	query = """
	--Get All Subject Lines
	SELECT 
		vars.value.text as SubjectLine,
		study.id as Messageid
	FROM
		(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """'))) 
	WHERE vars.name="Subject"
	"""
	return query

def create_email_message_id_query(startDate, endDate, appId):
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

# UNIQUE QUERIES 

# THIS IS INTENTIONALLY WRONG. We group on EventTime to mimic bad analytics since we consider uniques per day not uniquers per user.
def create_unique_message_events_query(startDate, endDate, appId):
	query = """
	--Unique Events for Messages
	SELECT
		user_id,
		INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
		SUBSTR(states.events.name, 20) AS Event,
		count(*) as Unique_Occurrence,
		SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS EventTime
	FROM
		(TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """')))
	WHERE states.events.name LIKE ".m%"
	GROUP BY user_id, MessageId, Event, EventTime
	"""
	return query

def join_unique_message_with_events(startDate, endDate, appId):
	query = """
	--JOIN messageEvents with MessageId's
	SELECT
		Session.MessageId,
		Session.user_id,
		CASE
			WHEN Session.Event="" THEN "Sent"
			ELSE Session.Event
			END AS Session.Event,
		COUNT(Session.Event) as Occurrence
	FROM
		(""" + create_unique_message_events_query(startDate,endDate,appId) + """) Session
	INNER JOIN
		(""" + create_message_id_query(startDate,endDate,appId) + """) Study
	ON Session.MessageId = Study.MessageId
	"""
	return query

def pivot_unique_subject_query(startDate, endDate, appId):
	query="""
	--Pivot
	SELECT
		Session.MessageId as MessageId,
		SUM(IF(Session.Event="Open", Occurrence, 0)) AS Unique_Open,
		SUM(IF(Session.Event="Click", Occurrence, 0)) AS Unique_Click
	FROM
		(""" + join_unique_message_with_events(startDate, endDate, appId) + """)
	GROUP BY MessageId
	"""
	return query

# BASIC QUERIES 
def create_message_events_query(startDate, endDate, appId):
	query = """
	--GRAB all message events
	SELECT
		INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
		SUBSTR(states.events.name, 20) AS Event,
		count(*) as Occurrence,
		SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS EventTime
	FROM
		(TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
			TIMESTAMP('""" + startDate + """'),
			TIMESTAMP('""" + endDate + """')))
	WHERE states.events.name LIKE ".m%"
	GROUP BY MessageId, Event, EventTime
	"""
	return query

def join_message_with_event(startDate, endDate, appId):
	query = """
	--JOIN messageEvents with MessageId's
	SELECT
		Session.MessageId as MessageId,
		--Session.user_id,
		CASE
			WHEN Session.Event="" THEN "Sent"
			ELSE Session.Event
			END AS Event,
		Session.Occurrence as Occurrence
	FROM
		(""" + create_message_events_query(startDate,endDate,appId) + """) Session
	INNER JOIN
		(""" + create_email_message_id_query(startDate,endDate,appId) + """) Study
	ON Session.MessageId = Study.MessageId
	"""
	return query

def pivot_subject_query(startDate, endDate, appId):
	query = """
	--Pivot
		SELECT
			MessageInfo.MessageId as MessageId,
			SUM(IF(MessageInfo.Event="Sent", MessageInfo.Occurrence, 0)) AS Sent,
			SUM(IF(MessageInfo.Event="Delivered", MessageInfo.Occurrence, 0)) AS Delivered,
			SUM(IF(MessageInfo.Event="Open", MessageInfo.Occurrence, 0)) AS Open,
			SUM(IF(MessageInfo.Event="Click", MessageInfo.Occurrence, 0)) AS Click,
			SUM(IF(MessageInfo.Event="Bounce", MessageInfo.Occurrence, 0)) AS Bounce,
			SUM(IF(MessageInfo.Event="Dropped", MessageInfo.Occurrence, 0)) AS Dropped,
			SUM(IF(MessageInfo.Event="Block", MessageInfo.Occurrence, 0)) AS Block,
			SUM(IF(MessageInfo.Event="Unsubscribe", MessageInfo.Occurrence, 0)) AS Unsubscribe
		FROM
			(""" + join_message_with_event(startDate,endDate,appId) + """) MessageInfo
		GROUP BY MessageId
	"""
	return query