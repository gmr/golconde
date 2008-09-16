#!/usr/bin/python
# -*- coding: utf-8 -*-

# @summary Golconde Command Line Utility
# @since 2008-09-13
# @author: Gavin M. Roy <gmr@myyearbook.com>


import optparse, psycopg2, sys, getpass
from optparse import OptionGroup

def main():

	# Set our various display values for our Option Parser
	usage = "usage: %prog [options]"
	version = "%prog 0.1"
	description = "Golconde command line utility to manage PostgreSQL origin server"

	# Create our parser and setup our command line options
	parser = optparse.OptionParser(usage=usage,version=version,description=description,conflict_handler='resolve')
	parser.add_option('--dbname', '-d', help='specify database name to connect to')
	parser.add_option('--schema', '-n', default='public', help='schema name to act upon (default: public)')
	parser.add_option('--table', '-t', help='table name to act upon')
	dbGroup = OptionGroup(parser, 'PostgreSQL Connection options')
	dbGroup.add_option('--host', '-h', default='localhost', help='database server host or socket directory (default: localhost)')
	dbGroup.add_option('--port', '-p', type='int', default=5432, help='database server port (default: 5432)')
	dbGroup.add_option('--user', '-U', default='postgres', help='database user name (default: postgres)')
	dbGroup.add_option('-W', action="store_true", default=False, help='force password prompt (should happen automatically)')
	parser.add_option_group(dbGroup)
	group = OptionGroup(parser, 'Golconde actions')
	group.add_option('--add', '-a', action="store_true", help='add the golconde trigger to a table for distribution')
	group.add_option('--remove' , '-r', action="store_true", help='remove galconde trigger from table to stop distribution')

	group.add_option('--queue', '-o', action="store_true", help='use an ActiveMQ Queue for a single consumer/client server')
	group.add_option('--topic' , '-m', action="store_true", help='use an ActiveMQ Topic for multiple consumers/client servers')

	group.add_option('--static', '-s', action="store_true", help='Use staticly compiled connection data for talking to ActiveMQ')
	group.add_option('--dynamic' , '-y', action="store_true", help='Use the golconde.settings table to specify connection data')

	parser.add_option_group(group)	
	
	# Parse the command line options
	(options, args) = parser.parse_args()	
		
	# Validate the command line options	
	if options.add and options.remove:
		print "Error: Both add and remove passed via the command line as an option"
		sys.exit()
		
	if not options.add and not options.remove:
		print "Error: You must specify an action to perform"
		sys.exit()

	if options.add and not options.queue and not options.topic:
		print "Error: You must specify the use of single queue or a topic for publishing data"
		sys.exit()

	if options.add and not options.static and not options.dynamic:
		print "Error: You must specify if you would like to use a statically compiled function or dynamic obtained connection data from golconde.settings for connecting to ActiveMQ"
		sys.exit()

	if not options.table:
		print "Error: You must specify a table name to act upon"
		sys.exit()

	if not options.dbname:
		print "Error: you must specify a database name to connect to"
		sys.exit()

	# Build the base DSN	
	dsn = "host='%s' port='%i' user='%s' dbname='%s'" % (options.host, options.port, options.user, options.dbname)

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
			
	# Set our Isolation Level
	pgsql.set_isolation_level(0)
	 
	# Create our cursor to perform the work with			
	cursor = pgsql.cursor()
		
	# If we're adding a trigger, run the function
	if options.add:
		createTrigger(options, cursor)
	
	# If we're removing a trigger, run the function
	if options.remove:
		dropTrigger(options, cursor)
	
# Build and apply the trigger to call the function in the database 
def createTrigger(options, cursor):
	
	# Build the query to get the schema data 
	query = "SELECT f.attnum AS number, f.attname AS name, f.attnum, f.attnotnull AS notnull, pg_catalog.format_type(f.atttypid,f.atttypmod) AS type, CASE WHEN p.contype = 'p' THEN 't' ELSE 'f' END AS primarykey, CASE WHEN p.contype = 'u' THEN 't' ELSE 'f' END AS uniquekey, CASE WHEN p.contype = 'f' THEN g.relname  END AS foreignkey, CASE WHEN p.contype = 'f' THEN p.confkey  END AS foreignkey_fieldnum, CASE WHEN p.contype = 'f' THEN g.relname END AS foreignkey, CASE WHEN p.contype = 'f' THEN p.conkey END AS foreignkey_connnum, CASE WHEN f.atthasdef = 't' THEN d.adsrc END AS default FROM pg_attribute f JOIN pg_class c ON c.oid = f.attrelid JOIN pg_type t ON t.oid = f.atttypid LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY ( p.conkey ) LEFT JOIN pg_class AS g ON p.confrelid = g.oid WHERE c.relkind = 'r'::char AND n.nspname = '%s' AND c.relname = '%s' AND f.attnum > 0 ORDER BY number;" % (options.schema, options.table)
	
	# Execute the query
	cursor.execute(query)
	rowCount = cursor.rowcount
	rows = cursor.fetchall()

	# Create the function	
	triggerFunction = ['CREATE OR REPLACE FUNCTION %s.%s_trigger_function() RETURNS trigger AS\n$BODY$\n' % (options.schema, options.table)]
	triggerFunction.append("if TD['event'] == 'INSERT':\n")

	# Insert logic
	triggerFunction.append("\tsql = \"INSERT INTO %s.%s (" % (options.schema, options.table))
	
	# Append the column names to insert into
	x = 0
	for row in rows:
		triggerFunction.append(row[1])	
		x += 1
		if x < rowCount:
			triggerFunction.append(',')
			
	triggerFunction.append(') VALUES (')
	
	# Append the placeholders to replace content into
	x = 0
	for row in rows:
		triggerFunction.append("'%s'")
		x += 1
		if x < rowCount:
			triggerFunction.append(',')
  	
	triggerFunction.append(");\" % (")

	# Append the plpythonu variables that will be replaced in our previously constructed string
	x = 0
	for row in rows:
		triggerFunction.append("TD['new']['%s']" % row[1])
		x += 1
		if x < rowCount:
			triggerFunction.append(',')
	triggerFunction.append(')\n')
	# End Insert Logic

	# Count the primary keys
	pkCount = 0
	for row in rows:
		if row[5] == 't':
			pkCount += 1
	
	# Update logic
	triggerFunction.append("\nif TD['event'] == 'UPDATE':\n")
	triggerFunction.append("\tsql = \"UPDATE %s.%s SET " % (options.schema, options.table))

	# Create the columns to update and the placeholders for the values
	x = 0
	for row in rows:
		triggerFunction.append("%s = '" % row[1])
		triggerFunction.append("%s'")	
		x += 1
		if x < rowCount:
			triggerFunction.append(',')

	triggerFunction.append(' WHERE ')

	# Create the variable spots for primary key columns for the where clause
	x = 0
	for row in rows:
		if row[5] == 't':
			triggerFunction.append("%s = '" % row[1])
			triggerFunction.append("%s'")
			x += 1
			if x < pkCount:
				triggerFunction.append(' AND ')		
				
	triggerFunction.append('" % (')

	# Append the plpythonu variables that will be replaced in our column update areas
	for row in rows:
		triggerFunction.append("TD['new']['%s']," % row[1])

	# Append the plpythonyu variables that will be replaced into our where clause
	for row in rows:
		if row[5] == 't':
			triggerFunction.append("TD['old']['%s']" % row[1])
			x += 1
			if x < pkCount:
				triggerFunction.append(',')

	triggerFunction.append(')\n')
	# End Update Logic
	
	# Delete Logic
	triggerFunction.append("\nif TD['event'] == 'DELETE':\n")
	triggerFunction.append("\tsql = \"DELETE FROM %s.%s WHERE " % (options.schema, options.table))
	
	# Create the variable spots for primary key columns for the where clause
	x = 0
	for row in rows:
		if row[5] == 't':
			triggerFunction.append("%s = '" % row[1])
			triggerFunction.append("%s'")
			x += 1
			if x < pkCount:
				triggerFunction.append(' AND ')		
				
	triggerFunction.append('" % (')
	
	# Append the plpythonyu variables that will be replaced into our where clause
	for row in rows:
		if row[5] == 't':
			triggerFunction.append("TD['old']['%s']" % row[1])
			x += 1
			if x < pkCount:
				triggerFunction.append(',')

	triggerFunction.append(')\n')
	# End Delete Logic

	# End of Trigger Function	
	triggerFunction.append('\n\nquery = "SELECT golconde.add_')
	
	# Build the center parts of the function name to call based upon our options
	if options.queue:
		triggerFunction.append('queue_')
	elif options.topic:
		triggerFunction.append('topic_')

	if options.static:
		triggerFunction.append('static_')
	elif options.dynamic:
		triggerFunction.append('dynamic_')
	
	# Add the rest of the trigger function
	triggerFunction.append('statement(\'%s.%s\'::text,' % (options.schema, options.table))
	triggerFunction.append('$$%s$$)" % sql\nplpy.execute(query)\n$BODY$\nLANGUAGE \'plpythonu\' VOLATILE\nCOST 100;')
	
	# Assemble the Trigger Function SQL
	query = ''.join(triggerFunction)
	
	# Create the Trigger Function
	print "Creating trigger function: %s.%s_trigger_function()" % ( options.schema, options.table )
	cursor.execute(query)
		
	# Create the Trigger
	print "Creating trigger: %s.%s_trigger" % ( options.schema, options.table )
	trigger = "CREATE TRIGGER %s_trigger AFTER INSERT OR UPDATE OR DELETE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE %s.%s_trigger_function();" % ( options.table, options.schema, options.table, options.schema, options.table)
	cursor.execute(trigger)
	
# Build and apply the trigger to call the function in the database 
def dropTrigger(options, cursor):	

	# Dropping the Trigger
	print "Dropping trigger: %s.%s_trigger" % ( options.schema, options.table )
	trigger = "DROP TRIGGER %s_trigger ON %s.%s;" % ( options.table, options.schema, options.table )
	cursor.execute(trigger)	

	# Dropping the Trigger Function
	print "Dropping trigger function: %s.%s_trigger_function()" % ( options.schema, options.table )
	trigger = "DROP FUNCTION %s.%s_trigger_function();" % ( options.schema, options.table )
	cursor.execute(trigger)	
	
if __name__ == "__main__":
    main()		