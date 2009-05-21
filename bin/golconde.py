#!/usr/bin/env python

""" 
Golconde Consumer Client

Listens to incoming commands, performing the appropriate queueing for subsequent
actions.    

@author Gavin M. Roy <gmr@myyearbook.com>
@since 2008-12-14
@requires Python 2.6
"""

import json, logging, mimetypes, optparse, psycopg2, os, resource, stomp, sys, thread, threading, time, yaml
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn

version = '0.6'

class AutoSQL(object):

    """
    AutoSQL Class

    This class is responsible for the behavior of turning messages into SQL 
    statements.    We do this by inspecting the schema of a target table and
    building base query structures for each action type
    """
    
    def __init__(self, cursor, target):
    
        # We expect a connected functional cursor
        self.cursor = cursor
        
        self.stats = {'add':0,'delete':0,'set':0,'update':0,'command_duration':0, 'errors':0}
        
        # If the schema was not specified assume it's public
        if '.' not in target:
            self.schema_name = 'public'
            self.table_name = target
        else:
            (self.schema_name, self.table_name) = target.split('.')
        
        # Get the Schema for our target
        query = """SELECT att.attnum AS "number", att.attname AS "name",
                        att.attnotnull AS not_null,
                        pg_catalog.format_type(att.atttypid, att.atttypmod) AS "type",
                        bool_or((con.contype = 'p') IS TRUE) AS primary_key,
                        bool_or((con.contype = 'u') IS TRUE) AS unique_constraint,
                        bool_or(ind.indisunique IS TRUE) AS unique_index
             FROM pg_catalog.pg_attribute att
             JOIN pg_catalog.pg_class rel ON rel.oid = att.attrelid
             JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
             LEFT JOIN pg_catalog.pg_constraint con
                                 ON con.conrelid = rel.oid
                                        AND att.attnum = ANY(con.conkey)
                                        AND con.contype IN ('p', 'u')
             LEFT JOIN pg_catalog.pg_index ind
                                 ON ind.indrelid = rel.oid
                                        AND att.attnum = ANY(ind.indkey)
                                        AND ind.indisunique
                                        AND ind.indisvalid
             WHERE rel.relkind = 'r'
                         AND att.attnum > 0
                         AND (nsp.nspname, rel.relname) = ('%s', '%s')
             GROUP BY att.attnum, att.attname, att.attnotnull,
                                att.atttypid, att.atttypmod
             ORDER BY att.attnum;""" % (self.schema_name, self.table_name)
        
        # Execute the query
        logging.debug('Fetching schema data for %s.%s' % (self.schema_name,self.table_name))
        cursor.execute(query)
        rowCount = cursor.rowcount
        self.schema = cursor.fetchall()

    # Process the message with our queries 
    def process(self, message):    
    
        if message['action'] == 'add':
    
            self.stats['add'] += 1
            
            # Create our empty lists
            fields = []
            values = []
            pk = []

            # Loop through our rows and see if we have values        
            for row in self.schema:
                column_name = row[1]
                column_type = row[3]
                primary_key = row[4]
                unique_constraint = row[5]
                unique_index = row[6]                
                
                # If our client passed in a value append our fields and values
                if message['data'].has_key(column_name):
                    fields.append(column_name)
                    fields.append(',')
                    if column_type in ['smallint','bigint','float','int','numeric']:
                        values.append("%s" % message['data'][column_name])
                    else:
                        values.append("$GOLCONDE$%s$GOLCONDE$" % message['data'][column_name])
                    values.append(',')
                    
                # Build our primary key / unique constraint / unique index string for return criterea
                if primary_key is True or unique_constraint is True or unique_index is True:
                    pk.append(column_name)
                    pk.append(',')

            # Remove the extra delimiters
            try:
                fields.pop()
                values.pop()
                pk.pop()
            except:
                logging.error('AutoSQL data packet decoding failed.  Message columns did not match schema.')
                sys.exit(1)
                
            
            # Build our query string
            field_string = ''.join(fields)
            value_string = ''.join(values)
            primary_key = ''.join(pk)
            query = 'INSERT INTO %s.%s (%s) VALUES (%s) RETURNING %s;' % (self.schema_name, 
                            self.table_name, field_string, value_string, primary_key)
            # Run our query
            try:
                logging.debug('AutoSQL.process(add) running: %s' % query)
                t = time.time()
                self.cursor.execute(query)
                self.stats['command_duration'] += time.time() - t
                     
            except psycopg2.OperationalError, e:
                # This is a serious error and we should probably exit the application execution stack with an error
                logging.error('PostgreSQL Error: %s' % e[0])
                sys.exit(1)
                
            except psycopg2.IntegrityError, e:
                # This is a PostgreSQL PK Constraint Error
                self.stats['errors'] += 1
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

            self.stats['delete'] += 1            

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
                        restriction.append("%s = $GOLCONDE$%s$GOLCONDE$" % ( column_name,message['restriction'][column_name]))
                    restriction.append(' AND ')

            # Remove the extra comma delimiter
            restriction.pop()
            
            # Build our query string
            restriction_string = ''.join(restriction)
            query = 'DELETE FROM %s.%s WHERE %s;' % (self.schema_name, self.table_name, restriction_string)
            
            # Try and execute our query
            try:
                logging.debug('AutoSQL.process(delete) running: %s' % query)
                t = time.time()
                self.cursor.execute(query)
                self.stats['command_duration'] += time.time() - t
                
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
            Set works by doing an upsert: Perform an update and if it fails, do an insert.    It does not 
            look for or respect restrictions, you would not use set to change primary key values
            """

            self.stats['set'] += 1
                    
            # Create our empty lists
            restriction = []
            values = []

            # Loop through our rows and see if we have values        
            for row in self.schema:
                column_name = row[1]
                column_type = row[3]
                primary_key = row[4]

                # If our client passed in a restriction append our fields and values
                if message['data'].has_key(column_name) and primary_key is True:
                    if column_type in ['smallint','bigint','float','int','numeric']:
                        restriction.append("%s = %s" % ( column_name, message['data'][column_name]))
                    else:
                        restriction.append("%s = $GOLCONDE$%s$GOLCONDE$" % ( column_name, message['data'][column_name]))
                    restriction.append(' AND ')

                # If our client passed in a value append our fields and values
                elif message['data'].has_key(column_name) and primary_key is not True:
                    if column_type in ['smallint','bigint','float','int','numeric']:
                        values.append("%s = %s" % (column_name, message['data'][column_name]))
                    else:
                        values.append("%s = $GOLCONDE$%s$GOLCONDE$" % (column_name, message['data'][column_name]))
                    values.append(',')

            if len(restriction) == 0:
                logging.debug('AutoSQL.process(set) Did not find a primary key, looking for a unique key')
                
                # Loop through our rows and see if have a unique key since we didn't pass a primary key 
                for row in self.schema:
                    column_name = row[1]
                    column_type = row[3]
                    unique_constraint = row[5]
                    unique_index = row[6]

                    # If our client passed in a restriction append our fields and values
                    if message['data'].has_key(column_name) and ( unique_constraint is True or unique_index is True ):
                        if column_type in ['smallint','bigint','float','int','numeric']:
                            restriction.append("%s = %s" % ( column_name, message['data'][column_name]))
                        else:
                            restriction.append("%s = $GOLCONDE$%s$GOLCONDE$" % ( column_name, message['data'][column_name]))
                        restriction.append(' AND ')            

            # If we found a primary key or unique queue, process the update, otherwise fall through to insert
            if len(restriction) > 0:
            
                # Remove the extra delimiters
                try:
                    restriction.pop()
                    values.pop()
                except:
                    logging.error('AutoSQL data packet decoding failed.  Message columns did not match schema.')
                    sys.exit(1)
                                
                # Build our query string
                restriction_string = ''.join(restriction)
                value_string = ''.join(values)
                query = 'UPDATE %s.%s SET %s WHERE %s;' % (self.schema_name, self.table_name, value_string, restriction_string)
                
                # Try and execute our query
                try:
                    logging.debug('AutoSQL.process(set[update]) running: %s' % query)
                    t = time.time()
                    self.cursor.execute(query)
                    self.stats['command_duration'] += time.time() - t
                                        
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
                    unique_constraint = row[5]
                    unique_index = row[6]                
                                    
                    # If our client passed in a value append our fields and values
                    if message['data'].has_key(column_name):
                        fields.append(column_name)
                        fields.append(',')
                        if column_type in ['smallint','bigint','float','int','numeric']:
                            values.append('%s' % message['data'][column_name])
                        else:
                            values.append("$GOLCONDE$%s$GOLCONDE$" % message['data'][column_name])
                        values.append(',')
                        
                    # Build our primary key string for return criterea
                    if primary_key is True or unique_constraint is True or unique_index is True:
                        pk.append(column_name)
                        pk.append(',')
    
                # Remove the extra delimiters
                try:
                    fields.pop()
                    values.pop()
                    pk.pop()
                except:
                    logging.error('AutoSQL data packet decoding failed.  Message columns did not match schema.')
                    sys.exit(1)
                                
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
            
            self.stats['update'] += 1
                
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
                        restriction.append("%s = %s" % ( column_name, message['restriction'][column_name]))
                    else:
                        restriction.append("%s = $GOLCONDE$%s$GOLCONDE$" % ( column_name, message['restriction'][column_name]))
                    restriction.append(' AND ')

                # If our client passed in a value append our fields and values
                if message['data'].has_key(column_name):
                    if column_type in ['smallint','bigint','float','int','numeric']:
                        values.append("%s = %s" % (column_name, message['data'][column_name]))
                    else:
                        values.append("%s = $GOLCONDE$%s$GOLCONDE$" % (column_name, message['data'][column_name]))
                    values.append(',')

            # Remove the extra delimiters
            try:
                restriction.pop()
                values.pop()
            except:
                logging.error('AutoSQL data packet decoding failed.  Message columns did not match schema.')
                sys.exit(1)
                
            # Build our query string
            restriction_string = ''.join(restriction)
            value_string = ''.join(values)
            query = 'UPDATE %s.%s SET %s WHERE %s;' % (self.schema_name, self.table_name, value_string, restriction_string)
            
            # Try and execute our query
            try:
                logging.debug('AutoSQL.process(update) running: %s' % query)
                t = time.time()
                self.cursor.execute(query)
                self.stats['command_duration'] += time.time() - t
                
            except psycopg2.OperationalError, e:
                logging.error('PostgreSQL Error: %s' % e[0])
                sys.exit(0)
            
            except Exception, e:
                # This is an error we don't know how to handle yet
                logging.error('PostgreSQL Error: %s' % e[0])
                sys.exit(1)
            
            # We successfully processed this message
            return json.dumps(message)
    
    # Return our stats dictionary
    def get_stats(self):
        return self.stats

class DestinationHandler(object):

    """
    Destination Queue Listener
    
    The destination object should process inbound packets, going to the canonical
    source, validating success, then distributing to the target queues
    """

    def __init__(self,name, config):

        # Set our values
        self.function = config['function_name']
        self.target = config['target']
        self.name = name
        self.config = config
        self.connected = False
    
        # Try to connect to our PostgreSQL DSN
        try:
            # Connect to the databases
            self.pgsql = psycopg2.connect(config['pgsql'])
            self.pgsql.set_isolation_level(0)
            self.connected = True
            
        # We encountered a problem
        except psycopg2.OperationalError, e:

            # Do string checks for various errors
            if 'Connection refused' in e[0]:
                logging.error("Error: Connection refusted to PostgreSQL for %s" % config['pgsql'])
                return
            
            if 'authentication failed' in e[0] or 'no password supplied' in e[0]:
                logging.error("Error: authentication failed")
                return
            
            # Unhandled exception, let the user know and exit the program
            if 'does not exist' in e[0]:
                logging.error("Error: Connection refusted to PostgreSQL for %s" % config['pgsql'])
                return
            
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
        
    def connect(self):
    
        logging.debug('In DestinationHandler connect()')
    
        # Variables to Connect to our Target stomp connections
        self.connections = 0
        self.destination_queue = []
        self.destination_connections = []
        
        # Connect for sending messages
        logging.debug('Adding targets')
        if self.config.has_key('Targets'):
            for (target_name, target_config) in self.config['Targets'].items():
                logging.debug("Connecting to destination connection target: %s" % target_name)
                self.destination_queue.append(target_config['queue'])
                (host,port) = target_config['stomp'].split(':')
                self.destination_connections.append(stomp.Connection([(host,int(port))]))
                self.destination_connections[self.connections].start()
                self.destination_connections[self.connections].connect()
                self.connections += 1
                                        
        logging.info('Destination Initialized')

    def on_connected(self, header, body):
        logging.info('Stomp Queue Connected on DestinationHandler, Session ID %s' % header['session'])

    def on_error(self, headers, message):
        log.error('Destination received an error from AMQ: %s' % message)

    def on_message(self, headers, message_in):
        message_out = self.message_processor.process(json.loads(message_in))
        if message_out != False:
            for i in range(0, self.connections):
                self.destination_connections[i].send(destination = self.destination_queue[i], message = message_out)
                
    def get_stats(self):
        return self.message_processor.get_stats()

class TargetHandler(object):
    
    """
    Target Queue Listener
    
    The destination object should process inbound target packets, processing
    messages sent from the Destination Handler and putting them in their proper 
    location
    """
    
    def __init__(self,name, config):
        global logging
        
        # Set our values
        self.function = config['function_name']
        self.target = config['target']
        self.name = name
        self.connected = False
    
        # Try to connect to our PostgreSQL DSN
        try:
            # Connect to the databases
            self.pgsql = psycopg2.connect(config['pgsql'])
            self.pgsql.set_isolation_level(0)
            self.connected = True
            
        # We encountered a problem
        except psycopg2.OperationalError, e:

            # Do string checks for various errors
            if 'Connection refused' in e[0]:
                logging.error("Error: Connection refusted to PostgreSQL for %s" % config['pgsql'])
                return
             
            if 'authentication failed' in e[0] or 'no password supplied' in e[0]:
                logging.error("Error: authentication failed")
                return
                
            # Unhandled exception, let the user know and exit the program
            if 'does not exist' in e[0]:
                logging.error("Error: Connection refusted to PostgreSQL for %s" % config['pgsql'])
                return
                
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
                print 'Undefined Destination Target Processing Function: %s' % self.function
                sys.exit(1)
                
        logging.info('Target Initialized')

    def on_connected(self, header, body):
        logging.info('Stomp Queue Connected on TargetHandler, Session ID %s' % header['session'])

    def on_error(self, headers, message):
        logging.error('Target received an error from AMQ: %s' % message)

    def on_message(self, headers, message_in):
        self.message_processor.process(json.loads(message_in))
        
    def get_stats(self):
        return self.message_processor.get_stats()        

class HTTPHandler(BaseHTTPRequestHandler):

    """
    Internal HTTP Server Handler Class
    
    Sends out JSON stats data about internal state
    """

    def send_data(self, response, mimetype):
        global version
        
        self.send_response(200)
        self.send_header('X-Server', 'Golconde/%s' % version)
        self.send_header('Content-type', mimetype)
        self.send_header('Content-length', len(response))
        self.end_headers()
        self.wfile.write(response)                
    
    def do_GET(self):
        global threads

        path = self.path.split('?')

        # Initial request for the stats ui
        if path[0] == '/':
            if os.path.isdir('assets'):
                if os.path.isfile('assets/index.html'):
                    f = open('assets/index.html', 'r')
                    response = f.read()
                    f.close()              
                    self.send_data(response, 'text/html')      
                else:
                    self.send_response(404)
            else:
                self.send_response(404)
            return
        
        if path[0].find('assets/') > 0:
            if os.path.isdir('assets'):
                if os.path.isfile(path[0][1:]):
                  
                    # Read in the file
                    f = open(path[0][1:], 'r')
                    response = f.read()
                    f.close()
    
                    # Get the mime type                
                    mime = mimetypes.guess_type(path[0][1:])
                   
                    # Send the response
                    self.send_data(response, mime[0])
                else:
                    self.send_response(404)
            else:
                self.send_response(404)
            return                            

        # 3rd party stub for json data
        elif path[0] == '/stats':
            
            # Build a list of thread stats returns
            thread_stats = []
            for thread in threads:
                thread_stats.append(thread.get_stats())
            
            stats = { 'worker_threads': len(threads), 'threads': thread_stats }           
            self.send_data(json.dumps(stats), 'application/json')
            return
            
        # Make the UI work with jsonp so we can serve the UI from a real webserver
        elif path[0] == '/stats/jsonp':
            
            # Build a list of thread stats returns
            thread_stats = []
            for thread in threads:
                thread_stats.append(thread.get_stats())
            
            stats = { 'worker_threads': len(threads), 'threads': thread_stats }           
            response = "jsonp_stats(%s);\n" %  json.dumps(stats)
            self.send_data(response, 'text/javascript')
            return
            
        # The running processes configuration
        elif path[0] == '/config':
            global config

            response = "jsonp_config(%s);\n" % json.dumps(config)
            self.send_data(response, 'text/javascript')
            return       

        else:
            self.send_error(404, 'File not found: %s' % self.path)            
        return

class DestinationThread(threading.Thread):

    """
    Destination Thread

    Thread that creates our stomp connection and its DestinationHandler object    
    """
    
    def __init__(self, name, config):
        threading.Thread.__init__(self)
        
        # Set internal variables
        self.name = name
        self.config = config

        # Create our Destination Handler Object    
        self.handler = DestinationHandler(self.name, self.config)
        self.connected = self.handler.connected

    def run(self):
        
        if self.connected is True:
          logging.info('Subscribing to Destination "%s" on queue: %s' % (self.name, self.config['queue']))
                  
          # Get our host and port to listen on        
          (host,port) = self.config['stomp'].split(':')
          
          # Connect to our stomp listener for the Destination
          destination_connection = stomp.Connection([(host,int(port))])
          destination_connection.add_listener(self.handler)
          destination_connection.start()
          destination_connection.connect()
          destination_connection.subscribe(destination=self.config['queue'],ack='auto')
        
        else:
          logging.info('Destination %s is disabled.' % self.name)
      
    def connect(self):
        # Connect our handler
        self.handler.connect()
            
    def get_stats(self):
        return { self.name: self.handler.get_stats() }
        
class TargetThread(threading.Thread):

    """
    Target Thread

    Thread that creates our stomp connection and its TargetHandler object    
    """

    def __init__(self, name, config):
        threading.Thread.__init__(self)
            
        # Set internal variables
        self.config = config    
        self.name = target_name

        # Create our Target Handler Object
        self.handler = TargetHandler(self.name, self.config)
        self.connected = self.handler.connected

    def run(self):
    
        logging.info('Subscribing to Target "%s" on queue: %s' % (target_name, target_config['queue']))

        # Get the host and port to listen on
        (host,port) = self.config['stomp'].split(':')

        # Connect to our stomp listener for the Target
        target_connection = stomp.Connection([(host,int(port))])
        target_connection.add_listener(self.handler)
        target_connection.start()
        target_connection.connect()
        target_connection.subscribe(destination=self.config['queue'],ack='auto')

    def get_stats(self):
        return { self.name: self.handler.get_stats() }

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""
  
# Set our various display values for our Option Parser
usage = "usage: %prog [options]"
version_string = "%%prog %s" % version
description = "Golconde command line daemon to listen to top level Golconde destination queues"

# Create our parser and setup our command line options
parser = optparse.OptionParser(usage=usage,
                                                             version=version_string,
                                                             description=description)
parser.add_option("-f", "--foreground",
                                action="store_true", dest="foreground", default=False,
                                help="Do not fork and stay in foreground")                                                                 
parser.add_option("-v", "--verbose",
                                action="store_true", dest="verbose", default=False,
                                help="use debug to stdout instead of logging settings")
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

# Set logging levels dictionary
logging_levels = {'debug': logging.DEBUG,
                                    'info': logging.INFO,
                                    'warning': logging.WARNING,
                                    'error': logging.ERROR,
                                    'critical': logging.CRITICAL}

# Get the logging value from the dictionary
logging_level = config['Logging']['level']
config['Logging']['level'] = logging_levels.get(config['Logging']['level'], logging.NOTSET)

# If the user says verbose overwrite the settings.
if options.verbose is True:
    if config['Logging']['filename']:
        del config['Logging']['filename']
    config['Logging']['level'] = logging.DEBUG
else:
    # Build a specific path to our log file
    config['Logging']['filename'] = "%s/%s/%s" % ( 
      config['Locations']['base'], 
      config['Locations']['logs'], 
      config['Logging']['filename'] )
  
# Pass in our logging config 
logging.basicConfig(**config['Logging'])
logging.info('Log level set to %s' % logging_level)

# Fork our process to detach if not told to stay in foreground
if options.foreground is False:
    try:
        pid = os.fork()
        if pid > 0:
            logging.info('Parent process ending.')
            sys.exit(0)            
    except OSError, e:
        sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Second fork to put into daemon mode
    try: 
        pid = os.fork() 
        if pid > 0:
            # exit from second parent, print eventual PID before
            print 'Golconde has started - PID # %d.' % pid
            logging.info('Child forked as PID # %d' % pid)
            sys.exit(0) 
    except OSError, e: 
        sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    logging.debug('After child fork')

    # Detach from parent environment
    os.chdir('/') 
    os.setsid()
    os.umask(0) 

    # Close stdin    	
    sys.stdin.close()
    
    # Redirect stdout, stderr
    sys.stdout = open('%s/%s/stdout.log' % ( config['Locations']['base'], config['Locations']['logs']), 'w')
    sys.stderr = open('%s/%s/stderr.log' % ( config['Locations']['base'], config['Locations']['logs']), 'w')    

# Main Thread Object for Stats
threads = []

# Loop through the destinations and kick off destination threads
logging.debug('Kicking off threads')
for (destination_name, destination_config) in config['Destinations'].items():

    # Destination thread
    logging.info('Creating new destination thread: %s' % destination_name)
    destination_thread = DestinationThread(destination_name, destination_config)
    logging.debug('Starting thread: %s' % destination_name)
    destination_thread.setName(destination_name)
    destination_thread.start()
    config['Destinations'][destination_name]['enabled'] = destination_thread.connected;

    # If our source is connected loop through the rest    
    if destination_thread.connected is True:
        threads.append(destination_thread)
    
        # Loop through the targets and kick off their threads
        if destination_config.has_key('Targets'):
            for ( target_name, target_config ) in destination_config['Targets'].items():
                logging.info('Creating new target thread: %s' % target_name)
                thread = TargetThread(target_name, target_config)
                logging.debug('Starting thread: %s' % target_name)
                thread.setName('%s_%s' % (destination_name, target_name))
                thread.start()
                if thread.connected is True:
                    threads.append(thread)
                config['Destinations'][destination_name]['Targets'][target_name]['enabled'] = thread.connected;
              
    # Now that we've setup our targets, connect to our parent queue and start working
    destination_thread.connect()    
    
# Start the HTTP Server
if config['HTTPServer']['enabled'] is True:
    server = ThreadedHTTPServer((config['HTTPServer']['listen'],config['HTTPServer']['port']), HTTPHandler)
    server.serve_forever()
else:
    """
    Just have the main loop run in a low CPU utilization mode, but keep us running while
    we receive messages from our Stomp server
    """
    while 1:
        time.sleep(1)