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
	MessageInfo.Unsubscribe as Unsubscribe,
	MessageInfo.Spam as Spam
FROM
	(""" + textwrap.indent(pivot_subject_query(startDate, endDate, appId, level + 1), '\t' * level) + """) MessageInfo
JOIN
	(""" + textwrap.indent(subject_line_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Subject
ON Subject.MessageId = MessageInfo.MessageId
GROUP BY Subject, MessageId, Sent, Delivered, Open, Click, Bounce, Dropped, Block, Unsubscribe, Spam
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
	study.id as MessageId
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
def create_unique_ab_query(startDate, endDate, appId, level=0):
	#Wrapper for ab
	return join_email_ab_unique_events_with_experiments(startDate, endDate, appId, level)

def create_ab_query(startDate,endDate,appId,level=0):
	#Wrapper for ab
	return join_email_ab_events_with_experiments(startDate, endDate, appId, level)

def create_experiment_message_query(startDate, endDate, appId, level=0):
	query="""--GET ALL VariantId's and their MessageId's
SELECT
	__key__.id as VariantId,
	SUBSTR(vars.name,12,16) as MessageId,
	vars.value.text as SubjectLine
FROM
	TABLE_DATE_RANGE([leanplum-staging:email_report_backups.Experiment_],
		TIMESTAMP('"""+startDate+"""'),
		TIMESTAMP('"""+endDate+"""'))
WHERE REGEXP_MATCH(vars.name,r'__message__([0-9]{16}).Subject$')
GROUP BY VariantId, MessageId"""
	return query

def join_email_ab_events_with_experiments(startDate, endDate, appId, level=0):
	query="""--Filter out incorrect experiments
SELECT
	Session.MessageId as MessageId,
	Session.ExperimentVariant as ExperimentVariant,
	Experiment.SubjectLine as SubjectLine,
  	SUM(IF(Session.Event="Sent", Session.Occurrence, 0)) as Sent,
  	SUM(IF(Session.Event="Delivered", Session.Occurrence, 0)) AS Delivered,
  	SUM(IF(Session.Event="Open", Session.Occurrence, 0)) AS Open,
  	SUM(IF(Session.Event="Click", Session.Occurrence, 0)) AS Click,
  	SUM(IF(Session.Event="Bounce", Session.Occurrence, 0)) AS Bounce,
  	SUM(IF(Session.Event="Dropped", Session.Occurrence, 0)) AS Dropped,
  	SUM(IF(Session.Event="Block", Session.Occurrence, 0)) AS Block,
  	SUM(IF(Session.Event="Unsubscribe", Session.Occurrence, 0)) AS Unsubscribe,
  	SUM(IF(Session.Event="Marked as spam", Session.Occurrence, 0)) AS Spam
FROM
	(""" + textwrap.indent(join_message_with_ab_events(startDate, endDate, appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_experiment_message_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Experiment
ON Session.ExperimentVariant = Experiment.VariantId WHERE STRING(Session.MessageId) = STRING(Experiment.MessageId)
GROUP BY MessageId, ExperimentVariant
ORDER BY MessageId, ExperimentVariant"""
	return query

def join_message_with_ab_events(startDate, endDate, appId, level=0):
	query="""--Filter for Email Messages
SELECT
	Session.MessageId as MessageId,
	Session.ExperimentVariant as ExperimentVariant,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Event,
	Session.Occurrence as Occurrence
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
	experiments.variant as ExperimentVariant,
	SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) * 1000000),0,10) AS EventTime
FROM
	FLATTEN(
		(SELECT 
			(*)
		FROM
			(TABLE_DATE_RANGE([leanplum2:leanplum.s_"""+ appId + """_],
				TIMESTAMP('""" + startDate + """'),
				TIMESTAMP('""" + endDate + """')))),experiments)
WHERE states.events.name LIKE ".m%"
GROUP BY MessageId, Event, EventTime, ExperimentVariant"""
	return query

# UNIQUE AB TEST QUERIES

def join_email_ab_unique_events_with_experiments(startDate, endDate, appId, level=0):
	query="""--Filter out incorrect experiments
SELECT
	Session.MessageId as MessageId,
	Session.ExperimentVariant as ExperimentVariant,
  	SUM(IF(Session.Event="Open", Session.Occurrence, 0)) AS Unique_Open,
  	SUM(IF(Session.Event="Click", Session.Occurrence, 0)) AS Unique_Click,
FROM
	(""" + textwrap.indent(join_message_with_unique_ab_events(startDate, endDate, appId, level + 1), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_experiment_message_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Experiment
ON Session.ExperimentVariant = Experiment.VariantId WHERE STRING(Session.MessageId) = STRING(Experiment.MessageId)
GROUP BY MessageId, ExperimentVariant
ORDER BY MessageId, ExperimentVariant"""
	return query


# THIS IS INTENTIONALLY WRONG. We group on EventTime to mimic bad analytics since we consider uniques per day not uniques per user.
def create_ab_message_unique_events_query(startDate, endDate, appId, level=0):
	query = """--Flatten Unique Experiments
SELECT
	user_id,
	INTEGER(SUBSTR(states.events.name,3,16)) AS MessageId,
	SUBSTR(states.events.name, 20) AS Event,
	COUNT(*) as Occurrence,
	experiments.variant as ExperimentVariant,
	SUBSTR(FORMAT_UTC_USEC(INTEGER(states.events.time) * 1000000),0,10) AS EventTime
FROM
	FLATTEN(
		(SELECT
			(*)
		FROM
			(TABLE_DATE_RANGE([leanplum2:leanplum.s_""" + appId + """_],
				TIMESTAMP('""" + startDate + """'),
				TIMESTAMP('""" + endDate + """')))),experiments)
WHERE states.events.name LIKE ".m%"
GROUP BY MessageId, user_id, Event, EventTime, ExperimentVariant"""
	return query
# Since in the ab_message_unique_events_query we are grouping by user_id we need to recount hence the Count in this one
def join_message_with_unique_ab_events(startDate, endDate, appId, level=0):
	query="""--Filter for Email Messages
SELECT
	Session.user_id as user_id,
	Session.MessageId as MessageId,
	Session.ExperimentVariant as ExperimentVariant,
	CASE
		WHEN Session.Event="" THEN "Sent"
		ELSE Session.Event
		END AS Event,
	COUNT(Session.Event) as Occurrence
FROM
	(""" + textwrap.indent(create_ab_message_unique_events_query(startDate,endDate,appId,level + 1 ), '\t' * level) + """) Session
INNER JOIN
	(""" + textwrap.indent(create_email_message_id_query(startDate, endDate, appId, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId
GROUP BY MessageId, user_id, ExperimentVariant, Event"""
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

# Since in the unique_message_events_query we are grouping by user_id we need to recount hence the COunt in this one.
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
	(""" + textwrap.indent(create_email_message_id_query(startDate,endDate,appId, level + 1), '\t' * level) + """) Study
ON Session.MessageId = Study.MessageId
GROUP BY Session.MessageId, Session.user_id, Session.Event"""
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
		SUM(IF(MessageInfo.Event="Unsubscribe", MessageInfo.Occurrence, 0)) AS Unsubscribe,
		SUM(IF(MessageInfo.Event="Marked as spam", MessageInfo.Occurrence, 0)) AS Spam,
	FROM
		(""" + textwrap.indent(join_message_with_event(startDate,endDate,appId, level + 1), '\t' * level) + """) MessageInfo
	GROUP BY MessageId"""
	return query