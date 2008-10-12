#!/usr/bin/python
# -*- coding: utf-8 -*-

# @summary Golconde Command Line Utility
# @since 2008-09-15
# @author: Gavin M. Roy <gmr@myyearbook.com>

import getpass, optparse, psycopg2, os, stomp, sys, time
from optparse import OptionGroup

# ActiveMQ Listener Class
#
# @todo Add exception handling
class TextListener(pyactivemq.MessageListener):
	def __init__(self):
		pyactivemq.MessageListener.__init__(self)

	# We we get a message, process it
	def onMessage(self, message):
		if self.verbose:
			print 'Query to execute: %s' % message.text
		self.cursor.execute(message.text)

	# Let the process pass in the cursor for the database
	def setCursor(self, cursor):
		self.cursor = cursor
	
	# How verbose are we?
	def setVerbose(self, verbose):
		self.verbose = verbose

def main():

	# Set our various display values for our Option Parser
	usage = "usage: %prog [options]"
	version = "%prog 0.2"
	description = "Golconde command line daemon to listen to a single topic or queue in ActiveMQ"

	# Create our parser and setup our command line options
	parser = optparse.OptionParser(usage=usage,version=version,description=description,conflict_handler='resolve')
	parser.add_option('--verbose', '-v', action="store_true", help='verbose debugging output')
	parser.add_option('--nofork', '-f', action="store_true", default=False, help='do not fork, run in foreground')
	
	group = OptionGroup(parser, 'PostgreSQL options')
	parser.add_option('--dbname', '-d', help='specify database name to connect to')
	parser.add_option('--schema', '-n', default='public', help='schema name to act upon (default: public)')
	parser.add_option('--table', '-t', help='table name to act upon')
	parser.add_option_group(group)

	group = OptionGroup(parser, 'PostgreSQL Connection options')
	group.add_option('--host', '-h', default='localhost', help='database server host or socket directory (default: localhost)')
	group.add_option('--port', '-p', type='int', default=5432, help='database server port (default: 5432)')
	group.add_option('--user', '-U', default='postgres', help='database user name (default: postgres)')
	group.add_option('-W', action="store_true", default=False, help='force password prompt (should happen automatically)')
	parser.add_option_group(group)
	
	group = OptionGroup(parser, 'ActiveMQ Connection options')
	group.add_option('--activemq', '-a', default='tcp://localhost:61613?wireFormat=stomp', help='ActiveMQ server to connect to (default: tcp://localhost:61613?wireFormat=stomp)')
	parser.add_option_group(group)
	
	group = OptionGroup(parser, 'Golconde options')
	group.add_option('--queue', '-o', action="store_true", help='use an ActiveMQ Queue for a single consumer/client server')
	group.add_option('--topic' , '-m', action="store_true", help='use an ActiveMQ Topic for multiple consumers/client servers')
	parser.add_option_group(group)

	# Parse the command line options
	(options, args) = parser.parse_args()	
	
	# Validate our various option combinations
	if not options.queue and not options.topic:
		print "Error: You must specify the use of single queue or a topic to subscribe to"
		sys.exit()

	if not options.nofork and options.verbose:
		print "Warning: Debug output with running golconde_client in the background is not very pretty."

	if not options.table:
		print "Error: You must specify a table name to act upon"
		sys.exit()

	if not options.dbname:
		print "Error: you must specify a database name to connect to"
		sys.exit()
		
	# Build the base DSN	
	dsn = "host='%s' port='%i' user='%s' dbname='%s'" % (options.host, options.port, options.user, options.dbname)

	# Debug
	if options.verbose:
		print 'Connecting to PostgreSQL via: %s' % dsn

	# Prompt for our password if we explicitly ask the program to do so
	if options.W:
		dsn = "%s password='%s'" % ( dsn, getpass.getpass('Password: ') )
	
	# Connect to the database, retrying if we get a no password supplied error
	while 1:
		
		# Try and connect, if we have a valid connection break out of the loop	
		try:
			pgsql = psycopg2.connect(dsn)
		
		# We encountered a problem
		except psycopg2.OperationalError, e:

			# Do string checks for various errors
			if 'Connection refused' in e[0]:
				print "Error: Connection refusted to PostgreSQL on %s:%i" % (options.host, options.port)
				sys.exit()
			
			if 'authentication failed' in e[0]:
				print "Error: authentication failed"
				sys.exit()
			
			# We need a password
			if 'no password supplied' in e[0]:
				dsn = "%s password='%s'" % ( dsn, getpass.getpass('Password: ') )
				continue

			# Unhandled exception, let the user know and exit the program
			raise
		
		# Everything is ok
		else:
			break

	# Fork unless we were told not to - love the double negative
	if not options.nofork:
		pid = os.fork()
		if pid:
			print "Child process %s created" % pid
			if options.verbose:
				print "Parent process stopping now"
			sys.exit()
		else:
			if options.verbose:
				print "Child process started"	
			
	# Try reconnecting to the database as the child if needed			
	if not options.nofork:
		try:
			pgsql = psycopg2.connect(dsn)
		
		# We encountered a problem
		except psycopg2.OperationalError, e:			
			print "Error reconnecting to the database as a child process."
			print e[0]
			sys(exit)			
			
	# Set our Isolation Level
	pgsql.set_isolation_level(0)
	 
	# Create our cursor to perform the work with			
	cursor = pgsql.cursor()

	# Debug
	if options.verbose:
		print 'Connecting to ActiveMQ via: %s' % options.activemq

	# Create our factory, connection, session, queue and producer
	factory = ActiveMQConnectionFactory(options.activemq)
	connection = factory.createConnection()
	session = connection.createSession(AcknowledgeMode.AUTO_ACKNOWLEDGE)
	
	# Debug
	if options.verbose:
		print 'Connected to ActiveMQ'

	# Create our topic or queue
	if options.queue:
		consume = session.createQueue('Golconde.%s.%s' % ( options.schema, options.table ) )	
	elif options.topic:
		consume = session.createTopic('Golconde.%s.%s' % ( options.schema, options.table ) )	
  
  # Create our consumer
	consumer = session.createConsumer(consume)

	# Create the listener
	listener = TextListener()  	
	listener.setCursor(cursor)
	listener.setVerbose(options.verbose)
	consumer.messageListener = listener
	connection.start()
	
	while 1:
		if options.verbose:
			print 'Sleeping in main loop for 10 seconds\r'		
		time.sleep(10)

if __name__ == "__main__":
    main()
