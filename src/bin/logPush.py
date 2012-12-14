#!bin/python

#### Pushes all but the most current log file to HDFS and moves to local archive folder
import os
import sys
import glob
import shutil
import subprocess
import logging
import inspect
from datetime import datetime

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
def init_logging( log_file, loglevel="DEBUG"):
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
log_file="/var/log/lwes-journaller/logPusher.log" # The log for this app, not to be confused with the lwesLogs this app is pushing
lwesLogs="/mnt/journals/current/"
lwesLogsProcessed="/mnt/journals/processed"
lwesLogHDFS="/user/gxetl/lwesLogs/"
init_logging( log_file )

if not os.path.isdir(lwesLogsProcessed):
	logging.error( "ERROR - archive directory does not exist, should already exist at: %s" % ( lwesLogsProcessed ))
	sys.exit(1)

#Check hdfs for log directory
subProc(\
	["hadoop", "fs", "-test","-d", lwesLogHDFS],
	"", "ERROR checking for hdfs dir %s" % (lwesLogHDFS),
	lineno() )

#Get the most recent file
dirList=os.listdir(lwesLogs)
logFiles=[]
#Make a list of only logFiles (remove directories)
# Should also check file name length and throw error if all files do not have same fileName length
for fname in dirList:
	fPath = lwesLogs+fname
	if os.path.isfile(fPath):
		logFiles.append(fname)

logFiles=sorted(logFiles)

# Move all but the max fileName into HDFS and then archive (the max filename is the most recent file and is currently being written to by journaller)
for i in range(len(logFiles) -1):
	# Check if this log already exists in HDFS, if so, log a warning and move file to archive
	sub=subprocess.Popen( ["hadoop", "fs", "-test", "-e", lwesLogHDFS+logFiles[i]] , stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	output = sub.communicate()[0]
	rc= sub.returncode
	if rc ==0:
		logging.warn( str(datetime.now())+"File %s already exists in lwesLogs" % ( logFiles[i] ))
		shutil.move( lwesLogs+logFiles[i], lwesLogsProcessed)

	# If the file doesn't exist (which it shouldn't) then copy it to HDFS and move it to local archives
	else:
		
		logging.debug("Moving this log File: %s " % logFiles[i])
		subProc(\
			["hadoop","fs", "-put", lwesLogs+logFiles[i], lwesLogHDFS],
			"", "ERROR - failed trying to push file to hdfs: %s" % ( logFiles[i] ), lineno() )
		shutil.move( lwesLogs+logFiles[i], lwesLogsProcessed)
		 
