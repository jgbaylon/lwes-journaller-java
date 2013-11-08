#!bin/python

#### Pushes all but the most current log file to HDFS and moves to local archive folder
import datetime
import inspect
import logging
import os
import shutil
import smtplib
import socket
import subprocess
import sys
import traceback
import uuid

#########################################
class logPush:
	log_level = "INFO"
	log_file = "/var/log/lwes-journaller/logPusher.log" # The log for this app, not to be confused with the local_lwes_logs this app is pushing
	lock_file = "/var/lock/lwes-journaller/logPusher"
	local_lwes_logs = "/journals/current/"
	local_lwes_logsProcessed = "/journals/processed"
	hdfs_temp_lwes_log = "/user/gxetl/tmp/lwesLogs/"
	hdfs_lwes_log = "/user/gxetl/lwesLogs/"
	WFID = str(uuid.uuid4())

	REPORTED_ERROR = 84 ## Error code to indicate original error has already been reported

	#########################################
	def __init__(self, logger_name=None, wfid=None):
		self.WFID = wfid or str(uuid.uuid4())

		## Create a util instance to be used by this script
		print "Start logging w/ logger = {0}".format(logger_name)
		self.init_logging(logger=logger_name, log_file=self.log_file, log_level=self.log_level)

	#########################################
	def init_logging(self, logger=None, log_file=None, log_level='INFO'):
		numeric_level = getattr(logging, log_level.upper(), None)
		# if a logger is passed in from a parent object than use that, otherwise create one
		if logger:
			print "using inherited logger"
			master_logger = logging.getLogger(logger)
		else:
			print "making my own logger with loging level {0}".format(numeric_level)
			master_logger = logging.getLogger("master")
			# if no log_file is specified than log to console
			if not log_file:
				masterHandler = logging.StreamHandler()
			else:
				masterHandler = logging.FileHandler(log_file, delay=True)
			master_logger.setLevel(numeric_level)
			formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
			masterHandler.setFormatter(formatter)
			master_logger.addHandler(masterHandler)
		self.master_logger = master_logger

	#########################################
	def subProc(self, args, start_msg, err_msg, line_no):
		if len(start_msg) > 1:
			self.master_logger.info(start_msg)
		sub = subprocess.Popen(args, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		output = sub.communicate()[0]
		rc = sub.returncode
		if rc != 0:
			self.report_error(err_msg+":\n"+output, 255, line_no)
			
	#########################################
	def lineno(self):
		###Returns the current line number
		return inspect.currentframe().f_back.f_lineno
	
	#########################################
	def report_error(self, msg, err_code, line_no):
		print "ERROR @ line {0}".format(line_no)
		print "ERROR Info: {0}".format(msg)

		self.master_logger.error("\nLINE: {0} \n{1}".format(line_no, msg))

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
		self.master_logger.error("REPORTING ERROR: \n{0}".format(msg))
		sys.exit(err_code)

	#########################################
	def report_error_new(self, msg, err_code, line_no):
		# Log the error a batch specific Error log
		msg = "WFID: {0} \nLINE: {1} \n{2}".format(self.WFID, line_no,  msg)
		self.log_msg( msg, self.error_log)
		# Log the Job log as well
		self.master_logger.error("REPORTING ERROR: \n{0}".format(msg))

		### Email the error
		hostname = socket.gethostname()
		SUBJECT = "{2} : ERROR IN: {0} @ line {1}" .format(self.cur_prg, line_no, hostname)
		TO = "alex@gradientx.com"
		FROM = "alex@gradientx.com"
		header = "To:{1}{0}From:{2}{0}Subject:{3}{0}".format("\n", TO, FROM, SUBJECT)
		BODY = header+"\n"+msg

		self.master_logger.info("Trying to establish server")
		try:
			server = smtplib.SMTP('127.0.0.1')
			self.master_logger.info("trying to send MAIL to {0}".format( TO))
			server.sendmail(FROM, [TO], BODY)
		except smtplib.SMTPException:
			self.master_logger.error("ERROR trying to send email")
			raise
		finally:
			server.quit()
		sys.exit(self.REPORTED_ERROR)

	#########################################
	def create_lock(self):
		self.master_logger.info("Creating lock file {0}".format(self.lock_file))
		with open(self.lock_file, 'w+') as f:
			f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

	#########################################
	def clear_lock(self):
		self.master_logger.info("Clearing lock for {0}".format(self.lock_file))
		if not os.path.exists(self.lock_file):
			self.master_logger.warning("Cant clear lock that doesn't exist")
			self.master_logger.error(self.pTime()+"Trying to clear a lock that does not exist: {0}".format(self.lock_file))
			sys.exit(1)
		os.remove(self.lock_file)

	#########################################
	### Return 1 if lock exists and process is running
	def check_lock(self):
		if os.path.exists(self.lock_file):
			self.master_logger.debug("LOCK EXISTS. Process is already running.")
			return 1
		else:
			return 0

	#########################################
	def check_directories(self):

		if not os.path.isdir(self.local_lwes_logsProcessed):
			self.master_logger.error("ERROR - Processed directory does not exist, should already exist at: {0}".format(self.local_lwes_logsProcessed ))
			sys.exit(1)
		#Check hdfs for log directory
		self.master_logger.debug("Checking HDFS log directories")
		self.subProc(["hadoop", "fs", "-test", "-d", self.hdfs_lwes_log], "", "ERROR checking for hdfs dir {0}".format(self.hdfs_lwes_log),
			self.lineno())
		self.master_logger.debug("Checking tmp directory {0}".format(self.hdfs_temp_lwes_log))
		#Check the temp directory (used to ensure the put is finished before accessing the file)
		sub = subprocess.Popen(["hadoop", "fs", "-test", "-d", self.hdfs_temp_lwes_log], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		output = sub.communicate()[0]
		rc = sub.returncode
		if rc != 0:
			self.master_logger.warn("Tmp directory, {0}, doesn't exist, making now".format(self.hdfs_temp_lwes_log))
			self.subProc(["hadoop", "fs", "-mkdir", self.hdfs_temp_lwes_log ],"", "ERROR making temp directory {0}".format(self.hdfs_temp_lwes_log),
				self.lineno())

	#########################################
	# check if the file exists in the temp dir, if yes then delete it
	def check_if_exists_in_temp_dir(self, log_file):

		sub = subprocess.Popen(["hadoop", "fs", "-test", "-e", os.path.join(self.hdfs_temp_lwes_log, log_file)],
		                       stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		sub.communicate()
		rc = sub.returncode
		# Return code of 0 means test passed and the file already exists, so it's already been processed.
		if rc == 0:
			self.master_logger.error("File {0} already exists in {1}. DELETING!".format(log_file, self.hdfs_temp_lwes_log))
			sub = subprocess.Popen(["hadoop", "fs", "-rm", os.path.join(self.hdfs_temp_lwes_log, log_file)],
			                       stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
			output = sub.communicate()[0]
			rc = sub.returncode
			if rc != 0:
				self.report_error("ERROR trying to remove log file, {0} from temp dir: {1}".format(log_file, output),
				                  85, self.lineno())

	#########################################
	# check if the file exists in the final lwesLogs dir, if yes then don't process this file
	def check_if_exists_in_log_dir(self, log_file):
		sub = subprocess.Popen(["hadoop", "fs", "-test", "-e", os.path.join(self.hdfs_lwes_log, log_file)],
		                       stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		sub.communicate()
		rc = sub.returncode
		# Return code of 0 means test passed and the file already exists, so it's already been processed.
		if rc == 0:
			self.master_logger.warn(str(datetime.datetime.now())+"File {0} already exists in {1}".format(
				log_file, self.hdfs_lwes_log))
			shutil.move(os.path.join(self.local_lwes_logs, log_file), self.local_lwes_logsProcessed)
			return True
		else:
			return False

	#########################################
	# Copy the log file from local file system to HDFS
	def put_log_to_hdfs(self, log_file):
		start = datetime.datetime.now()
		self.master_logger.info("Moving log File: {0}".format(log_file))

		## Put to temp directory
		self.subProc(["hadoop", "fs", "-put", os.path.join(self.local_lwes_logs, log_file), self.hdfs_temp_lwes_log], "",
		             "ERROR - failed trying to push file to hdfs: {0}".format(log_file), self.lineno())
		stop = datetime.datetime.now()
		diff = stop - start
		self.master_logger.info("Took {0} to move file {1}".format(str(diff.seconds / 60) + ':' + str(diff.seconds % 60).zfill(2), log_file))
		shutil.move(os.path.join(self.local_lwes_logs, log_file), self.local_lwes_logsProcessed)
		self.subProc(["hadoop","fs", "-mv", os.path.join(self.hdfs_temp_lwes_log, log_file), self.hdfs_lwes_log],
		             "", "ERROR - failed trying to move file to logDir: {0}".format(self.log_file), self.lineno())

	#########################################
	def main(self):
		
		# Sanity check
		self.check_directories()

		#Get the most recent file
		dirList = os.listdir(self.local_lwes_logs)
		logFiles = []

		#Make a list of only logFiles (remove directories)
		# Should also check file name length and throw error if all files do not have same fileName length
		for fname in dirList:
			fPath = self.local_lwes_logs+fname
			if os.path.isfile(fPath):
				logFiles.append(fname)

		logFiles = sorted(logFiles)
		self.master_logger.debug("Pushing {0} files to HDFS".format(len(logFiles) - 1))
		# Move all but the max fileName into HDFS and then archive (the max filename is the most recent file and is currently being written to by journaller)
		for i in range(len(logFiles) - 1):
			# First check if this file already exists in tmp directory - if so then the process was probably killed in mid copy so remove the old file
			self.check_if_exists_in_temp_dir(logFiles[i])

			# Check if this log already exists in HDFS, if so, log a warning and move file to archive
			already_exists = self.check_if_exists_in_log_dir(logFiles[i])
			# If the file doesn't exist (which it shouldn't) then copy it to HDFS and move it to local archives
			if not already_exists:
				self.put_log_to_hdfs(logFiles[i])

		self.master_logger.info("FINISHED for wfid {0}".format(self.WFID))

############################################
############## MAIN
############################################
if __name__ == "__main__":

	pusher = logPush()
	pusher.master_logger.info("Initiated Log Pusher")
	## Make sure process is not already running
	if pusher.check_lock() == 1:
		pusher.master_logger.info("wfid {0}: Found Lock, Exiting.".format(pusher.WFID))
		sys.exit(0)
	try:
		pusher.master_logger.debug("wfid {0}: About to create lock".format(pusher.WFID))
		pusher.create_lock()
		pusher.main()
		pusher.master_logger.debug("wfid {0}: About to clear lock".format(pusher.WFID))
		pusher.clear_lock()
	except SystemExit as e:
		pusher.master_logger.debug("CAUGHT SYSTEM EXIT %s" % (str(e)))
		if not str(e) == '0':
			pusher.master_logger.error("Caught System exit other than 0: %s, clearing lock!" % (str(e)))
			pusher.clear_lock()
	except:
		exc_type, exc_value, exc_traceback = sys.exc_info()
		msg = traceback.format_exc()
		pusher.master_logger.exception("New Error")
		pusher.clear_lock()
		pusher.report_error(msg, 1, pusher.lineno())
		raise

