Start to finish setup to run:

1. First install the things we need (version #'s are what i'm running on currently):
	* python 3+ (python 3.6.4)
	* homebrew (brew 1.6)
	* pip (9.0.3)
	* google-cloud-sdk (194.0.0) This includes:
		-bq (2.0.30)
		-core (2018.03.16)
		-gsutil (4.29)
	* requirements.txt:
		BigQuery-Python==1.13.0
		google-api-python-client==1.6.4
		httplib2==0.10.3
		oauth2client==4.1.2
		pyasn1==0.4.2
		pyasn1-modules==0.2.1
		python-dateutil==2.6.1
		rsa==3.4.2
		six==1.11.0
		uritemplate==3.0.0
		gcloud
		google.cloud

Install from scratch walkthrough. From terminal execute these individually:

/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
brew install python
brew cask install google-cloud-sdk
gcloud init
	*Choose leanplum for project and 'n' for compute engine/zone setup
pip install requirements.txt 
	*make sure you run this from the directory containing the requirements.txt
	*on some newer macs, pip is not installed. Try installing `brew install python2` as the package includes pip
gcloud auth application-default login

2. Ensure proper files exist

Make sure you have both the python script and the attributes text:
	'email_data_reports.py'
	AppID_Attr.txt

If you do not have the AppID_Attr.txt, it is okay as the script will create a new one.

3a. Run the python script.

When running the python script ensure that the pythong version is 3+. You can do this by running:
	python --version

If this doesn't return python 3.x.x try running:
	python3 --version

If still no luck, ask someone for help.

3b. Running the script

The proper format for running the python script is

	python email_data_reports.py -c <CompanyID> -r <Report Type 's' or 'd'> -ts <Time Start YYYYMMDD> -te <Time End YYYYMMDD>

An example run may look like this for plex:
	python email_data_reports.py -c 5419519383699456 -r d -ts 20180404 -te 20180411
This will create reports for the domain for all app's affiliated under the Plex company ID within your current directory.

To get the company ID navigate to:
	1.https://dashboard-dot-leanplum.appspot.com/dashboard/admin
	2.Under the 'Usage' page, in the 'Companies' category click the '+'

======================
Use:

To use the script you must be in a directory that contains:
	*The python script
	*(Optionally) The AppID_Attr.txt file

Execute this is terminal:
	python email_data_reports.py -c <CompanyID> -r <Report Type 's' or 'd'> -ts <Time Start YYYYMMDD> -te <Time End YYYYMMDD>

======================

When running a domain report you may be prompted with:

	"Please enter location of email attribute for <AppName> : <AppID>"

Since the recipients email is tied to a user attribute in the user's profile we need to look up its location in the database.
All user attributes have the form: 'attr#'
	So for Tinder an email could be = attr8
	Where as for Plex an email could be = attr27


To find the email location for the app:
	1. Go to: https://dashboard-dot-leanplum.appspot.com/_ah/stats/shell
	2a. Take the AppID from the prompt given above and place in this script:
	2b>

APP_ID = <INSERT APP ID HERE, example: 6471146988371968>
import model
app_data = model.AppData.get_by_id(APP_ID)
print "Attributes:"
for k, v in enumerate(app_data.attribute_columns):
 print "%s -> %s" % (k, v.encode('utf-8'))

 	3. Find the number that points to the email attribute that is being used.
 		If doing Tinder like mentioned above it might look like:
 		8 -> email

Now that you have the number, back in the prompt insert the number, without spaces, and hit enter. This will use the value in the database search and append the value to the AppID_Attr.txt for future use.

======================

Optional flags you can use when running the script include:

-p  'This will change the project location for saving the backups'
-d  'This will change the dataset location for saving the backups'
	*The above location will look like <project-location>/<dataset-location>
	*Default is: leanplum-staging/email_report-backups
-m  'These are the dataset backups to load into bigquery'
	*Default is: ['App','Study','Experiment']
-b 'This is where we search for the datasets listed above'
	*Default is 'leanplum_backups'


======================

Current People with Write Access:

Joe Ross - leanplum-staging
Cindy Joe - leanplum-staging
Monica Chan - leanplum-staging
