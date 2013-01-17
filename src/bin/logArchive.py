#!/bin/python

import os
import glob
import sys
import re
import logging
import shutil
from datetime import datetime, timedelta

#########################################
def subProc(  args, start_msg, err_msg, line_no ):
	if len(start_msg) >1:
		logging.info( start_msg )
	sub=subprocess.Popen(args, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	output = sub.communicate()[0]
	rc= sub.returncode
	if rc !=0:
		report_error( err_msg+":\n"+output, 255, line_no )
#########################################
def lineno( ):
	###Returns the current line number
	return inspect.currentframe().f_back.f_lineno
#########################################
def init_logging( log_file, loglevel="INFO"):
	numeric_level = getattr(logging, loglevel.upper(), None)
        if not isinstance(numeric_level, int):
                raise ValueError('Invalid log level: %s' % loglevel)
	logging.basicConfig(filename=log_file,format='%(asctime)s %(levelname)s: %(message)s',\
	datefmt='%Y-%m-%d %H:%M:%S', level= numeric_level)

#########################################
def report_error( msg, err_code, line_no ):
	print "ERROR @ line %d" % ( line_no )
	print "ERROR Info: %s" % ( msg )
	
	logging.error( "\nLINE: %s \n%s" % ( line_no,  msg))

	### Email the error
	""" When email on EC2 is enabled, should be emailing Errors rather than just logging them
	//CUR_PRG!! SUBJECT = "ERROR in: %s @ line %d" % ( CUR_PRG, line_no)
	TO = "alex@gradientx.com"
	FROM = email_from
	text = msg ## TODO: add some formatting to make email readable
	BODY = string.join((
		"From: %s" % FROM,
		"To: %s" % TO,
		"Subject: %s" % SUBJECT,
		"", text ), "\r\n")
	print "Trying to establish server"
	server = smtplib.SMTP('127.0.0.1')

	print "trying to send MAIL"
	server.sendmail(FROM, [TO], BODY)
	server.quit()
	"""
	logging.error("REPORTING ERROR: \n%s" % ( msg ))
	sys.exit(err_code)


############################################
############## MAIN
############################################
rententionDays=11
log_file="/var/log/lwes-journaller/logArchiver.log" # The log for this app, not to be confused with the lwesLogs this app is pushing
lwesLogs="/mnt/journals/current/"
lwesLogsArchive="/mnt/journals/archive/"
lwesLogsProcessed="/mnt/journals/processed/"
init_logging( log_file )

logging.info("START LOG ARCVIVER")
### Put all the processed files in archive directories
dirList = dirList=glob.glob( lwesLogsProcessed+'*')
for file in dirList:
	m = re.search('([0-9]{12})[^/]+seq', file)
	date = m.group(1)
	archiveDir = lwesLogsArchive+date[0:8]
	logging.debug("File %s has archive dir %s" % (file, archiveDir)) 
	if not os.path.exists( archiveDir ):
		logging.info( "Dir %s does not exist, making now" % (archiveDir) )
		os.makedirs( archiveDir )
	shutil.move( file, archiveDir )

### Cycle through all the archive directories and delete any older than retention
### Get the date string for the retention cutoff
cutoff= (datetime.now() - timedelta(days= rententionDays )).strftime('%Y%m%d')
logging.info("Cutoff Day = %s" % cutoff)

for name in os.listdir(lwesLogsArchive): 
	if os.path.isdir(os.path.join(lwesLogsArchive, name)):
		if name < cutoff:
			logging.info("Going to remove dir %s%s" % (lwesLogsArchive, name))
			shutil.rmtree( os.path.join(lwesLogsArchive, name))
		
