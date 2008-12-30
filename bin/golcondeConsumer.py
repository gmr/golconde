#!/usr/bin/env python

""" 
Golconde Consumer Client

Listens to incoming commands, performing the appropriate queueing for subsequent
actions.  

@author Gavin M. Roy <gmr@myyearbook.com>
@since 2008-12-14
"""

import optparse, psycopg2, stomp, sys, thread, time, yaml

class AutoSQL(object):
  """
  AutoSQL Class

  This class is responsible for the behavior of turning messages into SQL 
  statements.  We do this by inspecting the schema of a target table and
  building base query structures for each action type
  """
  
  def __init__(self, cursor, target):
  
    # We expect a connected functional cursor
    self.cursor = cursor
    
    # If the schema was not specified assume it's public
    if '.' not in target:
      schema = 'public'
      table = target
    else:
      (schema, table) = target.split('.')
    
    # Get the Schema for our target
    query = """SELECT f.attnum AS number, f.attname AS name, f.attnum,
        f.attnotnull AS notnull, pg_catalog.format_type(f.atttypid,f.atttypmod) AS type,
        CASE WHEN p.contype = 'p' THEN 't' ELSE 'f' END AS primarykey,
        CASE WHEN p.contype = 'u' THEN 't' ELSE 'f' END AS uniquekey,
        CASE WHEN p.contype = 'f' THEN g.relname END AS foreignkey,
        CASE WHEN p.contype = 'f' THEN p.confkey  END AS foreignkey_fieldnum, 
        CASE WHEN p.contype = 'f' THEN g.relname END AS foreignkey, 
        CASE WHEN p.contype = 'f' THEN p.conkey END AS foreignkey_connnum, 
        CASE WHEN f.atthasdef = 't' THEN d.adsrc END AS default 
       FROM pg_attribute f JOIN pg_class c ON c.oid = f.attrelid 
         JOIN pg_type t ON t.oid = f.atttypid 
         LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum 
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace 
         LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY ( p.conkey ) 
         LEFT JOIN pg_class AS g ON p.confrelid = g.oid 
       WHERE c.relkind = 'r'::char AND n.nspname = '%s' AND c.relname = '%s' AND f.attnum > 0 
       ORDER BY number;""" % (schema, table)
    
    # Execute the query
    print 'Fetching schema data for %s.%s' % (schema,table)
    cursor.execute(query)
    rowCount = cursor.rowcount
    rows = cursor.fetchall()
  
    # @todo Build the base queries for each of the action types  
    print rows

  # Process the message with our queries 
  def process(self, message):  
  
    if message.action == 'add':
      print 'autoSQL::add'
    
    elif message.action == 'delete':
      print 'autoSQL::delete'
      
    elif message.action == 'set':
      print 'autoSQL::set'

    elif message.action == 'update':
      print 'autoSQL::update'


class GolcondeDestinationHandler(object):
  """
  Golconde Destination Queue Listener
  
  The destination object should process inbound packets, going to the canonical
  source, validating success, then distributing to the target queues
  """

  def __init__(self,config):

    # Set our values
    self.function = config['function']
    self.target = config['target']
  
    print config
    # Try to connect to our PostgreSQL DSN
    try:
      # Connect to the databases
      self.pgsql = psycopg2.connect(config['pgsql'])
      self.pgsql.set_isolation_level(0)
      
    # We encountered a problem
    except psycopg2.OperationalError, e:

      # Do string checks for various errors
      if 'Connection refused' in e[0]:
        print "Error: Connection refusted to PostgreSQL for %s" % config.pgsql
        sys.exit(1)
      
      if 'authentication failed' in e[0] or 'no password supplied' in e[0]:
        print "Error: authentication failed"
        sys.exit(1)
      
      # Unhandled exception, let the user know and exit the program
      raise
      
    # Build our cursor to work with
    self.cursor = self.pgsql.cursor()
    
    if self.function == 'AutoSQL':
      self.auto_sql = AutoSQL(self.cursor, self.target)
    else:
      print 'Undefined Destination Authorative Processing Function: %s' % self.function
      sys.exit(1)
    
    print 'Destination Initialized'

  def on_error(self, headers, message):
    print 'received an error %s' % message

  def on_message(self, headers, message):
    self.auto_sql.process(message)

class GolcondeTargetHandler(object):
  
  """
  Golconde Target Queue Listener
  
  The destination object should process inbound target packets, processing
  messages sent from the Destination Handler and putting them in their proper 
  location
  """
  
  def __init__(self,config):
    print 'Initialized'

  def on_error(self, headers, message):
    print 'received an error %s' % message

  def on_message(self, headers, message):
    print 'received a message %s' % message


# Main Application Flow
def main():

  # Set our various display values for our Option Parser
  usage = "usage: %prog [options]"
  version = "%prog 0.3"
  description = "Golconde command line daemon to listen to top level Golconde destination queues"

  # Create our parser and setup our command line options
  parser = optparse.OptionParser(usage=usage,
                                 version=version,
                                 description=description)
  parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=True,
                  help="make lots of noise [default]")
  parser.add_option("-c", "--config", 
                  action="store", type="string", default="golconde.yaml", 
                  help="Specify the configuration file to load.")

  # Parse our options and arguments                  
  options, args = parser.parse_args()

  # Load the Configuration file
  try:
    stream = file(options.config, 'r')
    config = yaml.load(stream)
  except:
    print "Invalid or missing configuration file: %s" % options.config
    sys.exit(1)
    
  # Empty destination connection list to maintain a stack of connections we're using
  destination_connections = []
  connections = 0
  
  # Loop through the destinationsy
  for (destination, targets) in config['Destinations'].items():
    print 'Subscribing to Destination "%s" on queue: %s' % (destination, targets['queue'])
    
    # Connect to our stomplistener forthe Destination
    destination_connections.append(stomp.Connection())
    destination_connections[connections].add_listener(GolcondeDestinationHandler(targets))
    destination_connections[connections].start()
    destination_connections[connections].connect()
    destination_connections[connections].subscribe(destination=targets['queue'],ack='auto')
    connections += 1
  
  """
  Just have the main loop run in a low CPU utilization mode, but keep us running while
  we receive messages from our Stomp server
  """
  while (1):
    time.sleep(2)
  
if __name__ == "__main__":
	main()