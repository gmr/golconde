#!/usr/bin/env python

""" 
Golconde Consumer Client

Listens to incoming commands, performing the appropriate queueing for subsequent
actions.  

@author Gavin M. Roy <gmr@myyearbook.com>
@since 2008-12-14
"""

import logging, optparse, psycopg2, json, stomp, sys, thread, time, yaml

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
      self.schema_name = 'public'
      self.table_name = target
    else:
      (self.schema_name, self.table_name) = target.split('.')
    
    # Get the Schema for our target
    query = """SELECT f.attnum AS number, f.attname AS name,
        f.attnotnull AS notnull, pg_catalog.format_type(f.atttypid,f.atttypmod) AS type,
        CASE WHEN p.contype = 'p' THEN 't' ELSE 'f' END AS primarykey,
        CASE WHEN p.contype = 'u' THEN 't' ELSE 'f' END AS uniquekey
       FROM pg_attribute f JOIN pg_class c ON c.oid = f.attrelid 
         JOIN pg_type t ON t.oid = f.atttypid 
         LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum 
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace 
         LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY ( p.conkey ) 
       WHERE c.relkind = 'r'::char AND n.nspname = '%s' AND c.relname = '%s' AND f.attnum > 0 
       ORDER BY number;""" % (self.schema_name, self.table_name)
    
    # Execute the query
    logging.debug('Fetching schema data for %s.%s' % (self.schema_name,self.table_name))
    cursor.execute(query)
    rowCount = cursor.rowcount
    self.schema = cursor.fetchall()

  # Process the message with our queries 
  def process(self, message):  
  
    if message['action'] == 'add':

      # Create our empty lists
      fields = []
      values = []
      pk = []

      # Loop through our rows and see if we have values    
      for row in self.schema:
        column_name = row[1]
        column_type = row[3]
        primary_key = row[4]
        
        # If our client passed in a value append our fields and values
        if message['data'].has_key(column_name):
          fields.append(column_name)
          fields.append(',')
          if column_type in ['smallint','bigint','float','int','numeric']:
            values.append('%s' % message['data'][column_name])
          else:
            values.append("'%s'" % message['data'][column_name])
          values.append(',')
          
        # Build our primary key string for return criterea
        if primary_key == 't':
          pk.append(column_name)
          pk.append(',')

      # Remove the extra comma delimiter
      fields.pop()
      values.pop()
      pk.pop()
      
      # Build our query string
      field_string = ''.join(fields)
      value_string = ''.join(values)
      primary_key = ''.join(pk)
      query = 'INSERT INTO %s.%s (%s) VALUES (%s) RETURNING %s;' % (self.schema_name, 
              self.table_name, field_string, value_string, primary_key)
      # Run our query
      try:
        logging.debug('AutoSQL.process(add) running: %s' % query)
        self.cursor.execute(query)
        
      except psycopg2.OperationalError, e:
        # This is a serious error and we should probably exit the application execution stack with an error
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(1)
        
      except psycopg2.IntegrityError, e:
        # This is a PostgreSQL PK Constraint Error
        logging.error('PostgreSQL Error: %s' % e[0])
        return False
      
      except Exception, e:
        # This is an error we don't know how to handle yet
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(1)
      
      # Grab our returned PK data if needed
      pk_data = self.cursor.fetchone()
      pk_offset = 0
      
      # Loop through the rows and see if we need to add pk values to the message
      for row in self.schema:
        column_name = row[1]
        primary_key = row[4]
        
        # If our client passed in a value append our fields and values
        if not message['data'].has_key(column_name) and primary_key == 't':
          message['data'][column_name] = pk_data[pk_offset]
          pk_offset += 1   
      
      # We successfully processed this message
      return json.dumps(message)
    
    elif message['action'] == 'delete':
      
      # Create our empty lists
      restriction = []

      # Loop through our rows and see if we have values    
      for row in self.schema:
        column_name = row[1]
        column_type = row[3]
        
        # If our client passed in a value append our fields and values
        if message['restriction'].has_key(column_name):
          if column_type in ['smallint','bigint','float','int','numeric']:
            restriction.append("%s = %s" % ( column_name, message['restriction'][column_name]))
          else:
            restriction.append("%s = '%s'" % ( column_name,message['restriction'][column_name]))
          restriction.append(' AND ')

      # Remove the extra comma delimiter
      restriction.pop()
      
      # Build our query string
      restriction_string = ''.join(restriction)
      query = 'DELETE FROM %s.%s WHERE %s;' % (self.schema_name, self.table_name, restriction_string)
      
      # Try and execute our query
      try:
        logging.debug('AutoSQL.process(delete) running: %s' % query)
        self.cursor.execute(query)
        
      except psycopg2.OperationalError, e:
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(0)
      
      except Exception, e:
        # This is an error we don't know how to handle yet
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(1)
      
      # We successfully processed this message
      return json.dumps(message)
      
    elif message['action'] == 'set':
    
      """
      Set works by doing an upsert: Perform an update and if it fails, do an insert.  It does not 
      look for or respect restrictions, you would not use set to change primary key values
      """
    
      # Create our empty lists
      restriction = []
      values = []

      # Loop through our rows and see if we have values    
      for row in self.schema:
        column_name = row[1]
        column_type = row[3]
        primary_key = row[4]
        
        # If our client passed in a restriction append our fields and values
        if message['data'].has_key(column_name) and primary_key == 't':
          if column_type in ['smallint','bigint','float','int','numeric']:
            restriction.append('%s = %s' % ( column_name, message['data'][column_name]))
          else:
            restriction.append("%s = '%s'" % ( column_name, message['data'][column_name]))
          restriction.append(' AND ')

        # If our client passed in a value append our fields and values
        if message['data'].has_key(column_name) and primary_key != 't':
          if column_type in ['smallint','bigint','float','int','numeric']:
            values.append('%s = %s' % (column_name, message['data'][column_name]))
          else:
            values.append("%s = '%s'" % (column_name, message['data'][column_name]))
          values.append(',')

      if len(restriction) == 0:
        logging.debug('AutoSQL.process(set) Did not find a primary key, looking for a unique key')
        
        # Loop through our rows and see if have a unique key since we didn't pass a primary key 
        for row in self.schema:
          column_name = row[1]
          column_type = row[3]
          unique_key = row[5]
          
          # If our client passed in a restriction append our fields and values
          if message['data'].has_key(column_name) and unique_key == 't':
            if column_type in ['smallint','bigint','float','int','numeric']:
              restriction.append('%s = %s' % ( column_name, message['data'][column_name]))
            else:
              restriction.append("%s = '%s'" % ( column_name, message['data'][column_name]))
            restriction.append(' AND ')      

      # If we found a primary key or unique queue, process the update, otherwise fall through to insert
      if len(restriction) > 0:
      
        # Remove the extra delimiters
        restriction.pop()
        values.pop()
        
        # Build our query string
        restriction_string = ''.join(restriction)
        value_string = ''.join(values)
        query = 'UPDATE %s.%s SET %s WHERE %s;' % (self.schema_name, self.table_name, value_string, restriction_string)
        
        # Try and execute our query
        try:
          logging.debug('AutoSQL.process(set[update]) running: %s' % query)
          self.cursor.execute(query)
          
        except psycopg2.OperationalError, e:
          logging.error('PostgreSQL Error: %s' % e[0])
          sys.exit(0)
        
        except Exception, e:
          # This is an error we don't know how to handle yet
          logging.error('PostgreSQL Error: %s' % e[0])
          sys.exit(1)
      
      # Update failed, perform insert
      if self.cursor.rowcount <= 0 or len(restriction) == 0:
      
        # Create our empty lists
        fields = []
        values = []
        pk = []
  
        # Loop through our rows and see if we have values    
        for row in self.schema:
          column_name = row[1]
          column_type = row[3]
          primary_key = row[4]
          
          # If our client passed in a value append our fields and values
          if message['data'].has_key(column_name):
            fields.append(column_name)
            fields.append(',')
            if column_type in ['smallint','bigint','float','int','numeric']:
              values.append('%s' % message['data'][column_name])
            else:
              values.append("'%s'" % message['data'][column_name])
            values.append(',')
            
          # Build our primary key string for return criterea
          if primary_key == 't':
            pk.append(column_name)
            pk.append(',')
  
        # Remove the extra comma delimiter
        fields.pop()
        values.pop()
        pk.pop()
        
        # Build our query string
        field_string = ''.join(fields)
        value_string = ''.join(values)
        primary_key = ''.join(pk)
        query = 'INSERT INTO %s.%s (%s) VALUES (%s) RETURNING %s;' % (self.schema_name, 
                self.table_name, field_string, value_string, primary_key)
        # Run our query
        try:
          logging.debug('AutoSQL.process(set[add]) running: %s' % query)
          self.cursor.execute(query)
          
        except psycopg2.OperationalError, e:
          # This is a serious error and we should probably exit the application execution stack with an error
          logging.error('PostgreSQL Error: %s' % e[0])
          sys.exit(1)
          
        except psycopg2.IntegrityError, e:
          # This is a PostgreSQL PK Constraint Error
          logging.error('PostgreSQL Error: %s' % e[0])
          return False
        
        except Exception, e:
          # This is an error we don't know how to handle yet
          logging.error('PostgreSQL Error: %s' % e[0])
          sys.exit(1)
        
        # Grab our returned PK data if needed
        pk_data = self.cursor.fetchone()
        pk_offset = 0
        
        # Loop through the rows and see if we need to add pk values to the message
        for row in self.schema:
          column_name = row[1]
          primary_key = row[4]
          
          # If our client passed in a value append our fields and values
          if not message['data'].has_key(column_name) and primary_key == 't':
            message['data'][column_name] = pk_data[pk_offset]
            pk_offset += 1   
      
      # We successfully processed this message
      return json.dumps(message)

    elif message['action'] == 'update':
  
      # Create our empty lists
      restriction = []
      values = []

      # Loop through our rows and see if we have values    
      for row in self.schema:
        column_name = row[1]
        column_type = row[3]
        
        # If our client passed in a restriction append our fields and values
        if message['restriction'].has_key(column_name):
          if column_type in ['smallint','bigint','float','int','numeric']:
            restriction.append('%s = %s' % ( column_name, message['restriction'][column_name]))
          else:
            restriction.append("%s = '%s'" % ( column_name, message['restriction'][column_name]))
          restriction.append(' AND ')

        # If our client passed in a value append our fields and values
        if message['data'].has_key(column_name):
          if column_type in ['smallint','bigint','float','int','numeric']:
            values.append('%s = %s' % (column_name, message['data'][column_name]))
          else:
            values.append("%s = '%s'" % (column_name, message['data'][column_name]))
          values.append(',')

      # Remove the extra delimiters
      restriction.pop()
      values.pop()
      
      # Build our query string
      restriction_string = ''.join(restriction)
      value_string = ''.join(values)
      query = 'UPDATE %s.%s SET %s WHERE %s;' % (self.schema_name, self.table_name, value_string, restriction_string)
      
      # Try and execute our query
      try:
        logging.debug('AutoSQL.process(update) running: %s' % query)
        self.cursor.execute(query)
        
      except psycopg2.OperationalError, e:
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(0)
      
      except Exception, e:
        # This is an error we don't know how to handle yet
        logging.error('PostgreSQL Error: %s' % e[0])
        sys.exit(1)
      
      # We successfully processed this message
      return json.dumps(message)

class DestinationHandler(object):
  """
  Destination Queue Listener
  
  The destination object should process inbound packets, going to the canonical
  source, validating success, then distributing to the target queues
  """

  def __init__(self,config):

    # Set our values
    self.function = config['function']
    self.target = config['target']
  
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
      self.message_processor = AutoSQL(self.cursor, self.target)
    else:
      try:
        module = __import__(self.function)
        self.message_processor = getattr(module, 'process')
      except:
        print 'Undefined Destination Authorative Processing Function: %s' % self.function
        sys.exit(1)
    
    # Variables to Connect to our Target stomp connections
    self.connections = 0
    self.destination_queue = []
    self.destination_connections = []
    
    # Connect for sending messages
    if config.has_key('Targets'):
      for (target_name, target_config) in config['Targets'].items():
        self.destination_queue.append(target_config['queue'])
        (host,port) = target_config['stomp'].split(':')
        self.destination_connections.append(stomp.Connection([(host,int(port))]))
        self.destination_connections[self.connections].start()
        self.destination_connections[self.connections].connect()
        self.connections += 1
          
    logging.info('Destination Initialized')

  def on_error(self, headers, message):
    log.error('Destination received an error from AMQ: %s' % message)

  def on_message(self, headers, message_in):
    message_out = self.message_processor.process(json.loads(message_in))
    if message_out != False:
      for i in range(0, self.connections):
        self.destination_connections[i].send(destination = self.destination_queue[i], message = message_out)

class TargetHandler(object):
  
  """
  Target Queue Listener
  
  The destination object should process inbound target packets, processing
  messages sent from the Destination Handler and putting them in their proper 
  location
  """
  
  def __init__(self,config):
    # Set our values
    self.function = config['function']
    self.target = config['target']
  
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
      try:
        module = __import__(self.function)
        self.message_processor = getattr(module, 'process')
      except:
        print 'Undefined Destination Target Processing Function: %s' % self.function
        sys.exit(1)
        
    logging.info('Target Initialized')

  def on_error(self, headers, message):
    log.error('Target received an error from AMQ: %s' % message)

  def on_message(self, headers, message_in):
    self.auto_sql.process(json.loads(message_in))

def startDestinationThread(destination_name, target):
  # Connect to our stomp listener for the Destination
  logging.info('Subscribing to Destination "%s" on queue: %s' % (destination_name, target['queue']))
  (host,port) = target['stomp'].split(':')
  destination_connection = stomp.Connection([(host,int(port))])
  destination_connection.add_listener(DestinationHandler(target))
  destination_connection.start()
  destination_connection.connect()
  destination_connection.subscribe(destination=target['queue'],ack='auto')

def startTargetThread(target_name, target_config):
  # Connect to our stomp listener for the Target
  logging.info('Subscribing to Target "%s" on queue: %s' % (target_name, target_config['queue']))
  (host,port) = target_config['stomp'].split(':')
  target_connection =  stomp.Connection([(host,int(port))])
  target_connection.add_listener(TargetHandler(target_config))
  target_connection.start()
  target_connection.connect()
  target_connection.subscribe(destination=target_config['queue'],ack='auto')
   
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
    
  # Pass in our logging config    
  logging.basicConfig(**config['Logging'])
  
  # Loop through the destinations and kick off destination threads
  for (destination, destination_config) in config['Destinations'].items():
    thread.start_new_thread(startDestinationThread,(destination, destination_config))
    
    # Loop through the targets and kick off their threads
    if destination_config.has_key('Targets'):
      for ( target_name, target_config ) in destination_config['Targets'].items():
        thread.start_new_thread(startTargetThread,(target_name, target_config))
  
  """
  Just have the main loop run in a low CPU utilization mode, but keep us running while
  we receive messages from our Stomp server
  """
  while 1:
    time.sleep(1)
  
if __name__ == "__main__":
	main()