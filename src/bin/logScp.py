#!bin/python

#### Pushes via SCP all but the most current log file to Prod Entry Node and moves to local archive folder
import os
import sys
import glob
import shutil
import subprocess
import logging
import uuid
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

#########################################
def create_lock( ):
	logging.info( "Creating lock file %s" % (lock_File) )
	with open( lock_File, 'w+') as f:
		f.write( datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#########################################
def clear_lock( ):
	logging.info( "Clearing lock for  %s" % (lock_File))
	if not os.path.exists( lockFile ):
		self.masterLogger.warning("Cant clear lock that doesn't exist")
		self.masterLogger.error(self.pTime()+"Trying to clear a lock that does not exist: %s" % ( lockFile ))
		sys.exit(1)
	os.remove( lockFile )

#########################################
### Return 1 if lock exists and process is running
def check_lock( ):
	if os.path.exists( lock_File ):
		logging.debug( "LOCK EXISTS. Process is already running.")
		return 1
	else:
		return 0

#########################################
def main():
		
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
			
            logging.debug("SCPing this log File: %s " % logFiles[i])
            subProc(\
                ["scp", lwesLogs+logFiles[i], entryNode+":"+lwesLogEntryNode],
                "", "ERROR - failed trying to push file to hdfs: %s" % ( logFiles[i] ), lineno() )
            shutil.move( lwesLogs+logFiles[i], lwesLogsProcessed)



############################################
############## MAIN
############################################
log_file="/var/log/lwes-journaller/logScp.log" # The log for this app, not to be confused with the lwesLogs this app is pushing
lock_file="/var/lock/lwes-journaller/logScp"
lwesLogs="/mnt/journals/current/"
lwesLogsProcessed="/mnt/journals/processed"
entryNode="prod-hdfs-entry002.pe1i.gradientx.com"
lwesLogEntryNode="/home/gxetl/lwesLogs/"
WFID=str(uuid.uuid4())	
init_logging( log_file )

if not os.path.isdir(lwesLogsProcessed):
	logging.error( "ERROR - Processed directory does not exist, should already exist at: %s" % ( lwesLogsProcessed ))
	sys.exit(1)
## Make sure process is not already running
if ( check_lock() == 1):
		logging.info("Found Lock, Exiting on WFID %s" % ( WFID ))
		sys.exit(0)
	try:
		logging.debug("wfid: %s About to create lock" %( WFID))
		create_lock()
		main()
		logging.debug("wfid: %s About to clear lock" %( WFID))
		clear_lock(  )
	except SystemExit as  e:
		logging.debug( "CAUGHT SYSTEM EXIT %s" % ( str(e)))
		if not str(e) =='0':
			logging.error("Caught System exit other than 0: %s, clearing lock!" % ( str(e)))
			clear_lock( )
	except:
		exc_type, exc_value, exc_traceback = sys.exc_info()
		msg = traceback.format_exc()
		logging.exception( "New Error" )
		clear_lock(  )
		report_error( msg , 1, lineno() )
		raise

