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

import textwrap

# MAIN REPORT 
def create_subject_line_query(startDate, endDate, appId, level=0):
	query = """--JOIN Message Counts with Subject Line
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
	(""" + textwrap.indent(pivot_subject_query(startDate, endDate, appId, level + 1), '\t' * level) + """) MessageInfo
JOIN
	(""" + textwrap.indent(subject_line_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Subject
ON Subject.MessageId = MessageInfo.MessageId
GROUP BY Subject, MessageId, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe
ORDER BY MessageId"""
	return query

def create_unique_line_query(startDate, endDate, appId, level=0):
	#Wrapper
	return pivot_unique_subject_query(startDate, endDate, appId, level)


# GENERIC HELPERS 
def subject_line_query(startDate, endDate, appId, level=0):
	query = """--Get All Subject Lines
SELECT 
	vars.value.text as SubjectLine,
	study.id as Messageid
FROM
	(TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
		TIMESTAMP('""" + startDate + """'),
		TIMESTAMP('""" + endDate + """'))) 
WHERE vars.name="Subject" """
	return query

def create_email_message_id_query(startDate, endDate, appId, level=0):
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

# A/B Test Grouping

def create_ab_query(startDate,endDate,appId,level=0):
	#Wrapper for ab
	return join_email_ab_events_with_experiments(startDate, endDate, appId, level)

def create_experiment_message_query(startDate, endDate, appId, level=0):
	query="""--GET ALL ExperimentId's and their MessageId's
SELECT
	Study.ABTestId as ABTestId,
	Experiment.MessageId as MessageId
FROM
	(
	SELECT
		__key__.id as ABTestId,
		control.id as ControlId,
	FROM
		TABLE_DATE_RANGE([leanplum-staging.email_report_backups.Study_],
			TIMESTAMP('"""+startDate+"""'),
			TIMESTAMP('"""+endDate+"""'))
	GROUP BY ABTestId, ControlId) Study
JOIN
	(
	SELECT
		__key__.id as ControlId,
		SUBSTR(vars.name,12) as MessageId
	FROM
		TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
			TIMESTAMP('"""+startDate+"""'),
			TIMESTAMP('"""+endDate+"""'))
	GROUP BY ControlId, MessageId) Experiment
ON Study.ControlId = Experiment.ControlId"""
	return query

def join_email_ab_events_with_experiments(startDate, endDate, appId, level=0):
	query="""--Filter out incorrect experiments
SELECT
	Session.MessageId as MessageId,
	Session.ExperimentVariant as ExperimentVariant,
	Session.Event as Event,
	Session.Occurrence as Occurrence
FROM
	(""" + textwrap.indent(join_message_with_ab_events(startDate, endDate, appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_experiment_message_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Experiment
ON Session.ExperimentId = Experiment.ABTestId"""
	return query

def join_message_with_ab_events(startDate, endDate, appId, level=0):
	query="""--Filter for Email Messages
SELECT
	Session.MessageId as MessageId,
	Session.ExperimentId as ExperimentId,
	Session.ExperimentVariant as ExperimentVariant,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Event,
	COUNT(Session.Event) as Occurrence
FROM
	(""" + textwrap.indent(create_ab_message_events_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_email_message_id_query(startDate,endDate,appId,level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId"""
	return query

def create_ab_message_events_query(startDate, endDate, appId, level=0):
	query = """--Flatten experiments
SELECT
	INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
	SUBSTR(states.events.name, 20) AS Event,
	count(*) as Occurrence,
	experiments.id as ExperimentId,
	experiments.variant as ExperimentVariant,
	SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) *1000000),0,10) AS EventTime
FROM
	FLATTEN(
		(SELECT 
			(*)
		FROM
			(TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
				TIMESTAMP('""" + startDate + """'),
				TIMESTAMP('""" + endDate + """')))),experiments)
WHERE states.events.name LIKE ".m%"
GROUP BY MessageId, Event, EventTime, ExperimentId, ExperimentVariant"""
	return query

# UNIQUE QUERIES 

# THIS IS INTENTIONALLY WRONG. We group on EventTime to mimic bad analytics since we consider uniques per day not uniquers per user.
def create_unique_message_events_query(startDate, endDate, appId, level=0):
	query = """--Unique Events for Messages
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
GROUP BY user_id, MessageId, Event, EventTime"""
	return query

def join_unique_message_with_events(startDate, endDate, appId, level=0):
	query = """--JOIN messageEvents with MessageId's
SELECT
	Session.MessageId,
	Session.user_id,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Session.Event,
	COUNT(Session.Event) as Occurrence
FROM
	(""" + textwrap.indent(create_unique_message_events_query(startDate,endDate,appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_message_id_query(startDate,endDate,appId, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId"""
	return query

def pivot_unique_subject_query(startDate, endDate, appId, level=0):
	query="""--Pivot
SELECT
	Session.MessageId as MessageId,
	SUM(IF(Session.Event="Open", Occurrence, 0)) AS Unique_Open,
	SUM(IF(Session.Event="Click", Occurrence, 0)) AS Unique_Click
FROM
	(""" + textwrap.indent(join_unique_message_with_events(startDate, endDate, appId, level + 1), '\t' * level) + """)
GROUP BY MessageId"""
	return query

# BASIC QUERIES 
def create_message_events_query(startDate, endDate, appId, level=0):
	query = """--GRAB all message events
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
GROUP BY MessageId, Event, EventTime"""
	return query

def join_message_with_event(startDate, endDate, appId, level=0):
	query = """--JOIN messageEvents with MessageId's
SELECT
	Session.MessageId as MessageId,
	--Session.user_id,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Event,
	Session.Occurrence as Occurrence
FROM
	(""" + textwrap.indent(create_message_events_query(startDate,endDate,appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_email_message_id_query(startDate,endDate,appId, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId"""
	return query

def pivot_subject_query(startDate, endDate, appId, level=0):
	query = """--Pivot
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
		(""" + textwrap.indent(join_message_with_event(startDate,endDate,appId, level + 1), '\t' * level) + """) MessageInfo
	GROUP BY MessageId"""
	return query