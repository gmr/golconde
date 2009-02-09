#!/usr/bin/python
# -*- coding: utf-8 -*-

# @summary Golconde Command Line Utility
# @since 2008-09-13
# @author: Gavin M. Roy <gmr@myyearbook.com>

import optparse, psycopg2, simplejson as json, sys, getpass
from optparse import OptionGroup

def main():

  # Set our various display values for our Option Parser
  usage = "usage: %prog [options]"
  version = "%prog 0.3"
  description = "Golconde command line utility to manage PostgreSQL origin server triggers"
  
  # Create our parser and setup our command line options
  parser = optparse.OptionParser(usage=usage,version=version,description=description,conflict_handler='resolve')
  parser.add_option('--dbname', '-d', help='specify database name to connect to')
  parser.add_option('--schema', '-n', default='public', help='schema name to act upon (default: public)')
  parser.add_option('--table', '-t', help='table name to act upon')
  
  group = OptionGroup(parser, 'PostgreSQL Connection options')
  group.add_option('--host', '-h', default='localhost', help='database server host or socket directory (default: localhost)')
  group.add_option('--port', '-p', type='int', default=5432, help='database server port (default: 5432)')
  group.add_option('--user', '-U', default='postgres', help='database user name (default: postgres)')
  group.add_option('-W', action="store_true", default=False, help='force password prompt (should happen automatically)')
  parser.add_option_group(group)
  
  group = OptionGroup(parser, 'Message Queue Broker options')
  group.add_option('--broker_type', '-T', default='stomp', help='Broker type, currently only stomp is supported (default: stomp)')
  group.add_option('--broker_host', '-H', default='127.0.0.1', help='Broker ip address (default: 127.0.0.1)')
  group.add_option('--broker_port', '-P', type='int', default=61613, help='database server port (default: 61613)')
  parser.add_option_group(group)  
  
  group = OptionGroup(parser, 'Golconde options')
  group.add_option('--add', '-a', action="store_true", help='add the golconde trigger to a table for distribution')
  group.add_option('--remove' , '-r', action="store_true", help='remove galconde trigger from table to stop distribution')
  group.add_option('--queue' , '-q', default='auto', help='queue name to use.  If set to auto, will be schema_name.table_name')
  parser.add_option_group(group)	
  
  # Parse the command line options
  (options, args) = parser.parse_args()	

  # Validate the command line options	
  if options.add and options.remove:
    print "Error: Both add and remove passed via the command line as an option"
    parser.print_help()
    sys.exit()
  
  if not options.add and not options.remove:
    print "Error: You must specify an action to perform"
    parser.print_help()
    sys.exit()
  
  if not options.table:
    print "Error: You must specify a table name to act upon"
    parser.print_help()
    sys.exit()
  
  if not options.dbname:
    print "Error: you must specify a database name to connect to"
    parser.print_help()
    sys.exit()

  if not options.broker_host or not options.broker_port:
    print "Error: you must specify a broker host and port connect to"
    parser.print_help()
    sys.exit()

  if options.broker_type not in ['stomp']:
    print "Error: you must pick a valid broker type"
    parser.print_help()
    sys.exit()

  if options.queue == 'auto':
    options.queue = '%s.%s' % ( options.schema, options.table )

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

  # Buidl the primary key list
  primary_key = []
  for row in rows:
    if row[5] == 't':
      primary_key.append(row[1])

  # Add the entry to golconde.trigger_connections
  print 'Adding golconde.trigger_connections entry:'
  sql = "INSERT INTO golconde.trigger_connections VALUES ( '%s','%s','%s','%s',%i,'%s','%s' )" % \
    ( options.schema, options.table, options.broker_type, options.broker_host, options.broker_port, options.queue, json.dumps(primary_key) )
  print ' > %s' % sql
  cursor.execute(sql)

  # Create the Trigger
  print "Creating trigger: golconde.after_trigger_on_%s_%s" % ( options.schema, options.table ) 
  sql = 'CREATE TRIGGER golconde_after_trigger_on_%s_%s AFTER INSERT OR UPDATE OR DELETE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE golconde.trigger_function()' % \
    ( options.schema, options.table, options.schema, options.table )
  print ' > %s' % sql  
  cursor.execute(sql)
  

# Build and apply the trigger to call the function in the database 
def dropTrigger(options, cursor):	

  # Add the entry to golconde.trigger_connections
  print 'Removing golconde.trigger_connections entry:'
  sql = "DELETE FROM golconde.trigger_connections WHERE schema_name = '%s' AND table_name = '%s'" % ( options.schema, options.table )
  print ' > %s' % sql
  cursor.execute(sql)

  # Create the Trigger
  print "Dropping trigger: golconde.after_trigger_on_%s_%s" % ( options.schema, options.table ) 
  sql = 'DROP TRIGGER golconde_after_trigger_on_%s_%s ON %s.%s' % ( options.schema, options.table, options.schema, options.table )
  print ' > %s' % sql  
  cursor.execute(sql)
  
if __name__ == "__main__":
  main()		
