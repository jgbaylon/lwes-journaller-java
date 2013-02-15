#!bin/python

#### Pushes all but the most current log file to HDFS and moves to local archive folder
import os
import sys
import glob
import shutil
import subprocess
import logging
import uuid
import inspect
import traceback
import datetime
import socket

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

#########################################
def create_lock( ):
	logging.info( "Creating lock file %s" % (lock_file) )
	with open( lock_file, 'w+') as f:
		f.write( datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#########################################
def clear_lock( ):
	logging.info( "Clearing lock for  %s" % (lock_file))
	if not os.path.exists( lock_file ):
		logging.warning("Cant clear lock that doesn't exist")
		logging.error(self.pTime()+"Trying to clear a lock that does not exist: %s" % ( lock_file ))
		sys.exit(1)
	os.remove( lock_file )

#########################################
### Return 1 if lock exists and process is running
def check_lock( ):
	if os.path.exists( lock_file ):
		logging.debug( "LOCK EXISTS. Process is already running.")
		return 1
	else:
		return 0

def check_directories( ):
	#Check hdfs for log directory
        subProc(\
                ["hadoop", "fs", "-test","-d", lwesLogHDFS],
                "", "ERROR checking for hdfs dir %s" % (lwesLogHDFS),
                lineno() )
	logging.debug(" Checking tmp directory %s" % ( hdfsTempDir ))
	#Check the temp directory (used to ensure the put is finished before accessing the file)
	sub=subprocess.Popen( ["hadoop", "fs", "-test", "-d", hdfsTempDir], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	output = sub.communicate()[0]
	rc= sub.returncode
	if rc !=0:
		logging.warn("Tmp directory, %s, doesn't exist, making now" % (hdfsTempDir))
		subProc(\
			["hadoop", "fs", "-mkdir", hdfsTempDir ],
			"", "ERROR making temp directory %s" % (hdfsTempDir ),
			lineno() )
#########################################
def main():
	check_directories()

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
			logging.warn( str(datetime.datetime.now())+"File %s already exists in lwesLogs" % ( logFiles[i] ))
			shutil.move( lwesLogs+logFiles[i], lwesLogsProcessed)

		# If the file doesn't exist (which it shouldn't) then copy it to HDFS and move it to local archives
		else:
			start = datetime.datetime.now()
			logging.info("Moving log File: %s " % logFiles[i])
			## Put to temp directory
			subProc(\
				["hadoop","fs", "-put", lwesLogs+logFiles[i], hdfsTempDir],
				"", "ERROR - failed trying to push file to hdfs: %s" % ( logFiles[i] ), lineno() )
			stop = datetime.datetime.now()
			diff = stop - start
			logging.info("Took %s to move file %s" % ( str(diff.seconds / 60 ) + ':' + str(diff.seconds % 60).zfill(2), logFiles[i]))
			shutil.move( lwesLogs+logFiles[i], lwesLogsProcessed)
			subProc(\
                                ["hadoop","fs", "-mv", hdfsTempDir+logFiles[i], lwesLogHDFS],
                                "", "ERROR - failed trying to move file to logDir: %s" % ( logFiles[i] ), lineno() )

	logging.info("FINISHED for wfid %s" % ( WFID ))

############################################
############## MAIN
############################################
if __name__ == "__main__":
	global lock_file
	global WFID
	log_file="/var/log/lwes-journaller/logPusher.log" # The log for this app, not to be confused with the lwesLogs this app is pushing
	lock_file="/var/lock/lwes-journaller/logPusher"
	lwesLogs="/mnt/journals/current/"
	lwesLogsProcessed="/mnt/journals/processed"
	hdfsTempDir="/user/gxetl/tmp/lwesLogs/" 
	lwesLogHDFS="/user/gxetl/lwesLogs/"
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

