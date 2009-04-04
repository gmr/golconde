-- SQL Functions if you want to use Trigger Based replication

CREATE SCHEMA golconde;
GRANT USAGE ON SCHEMA golconde TO public;

-- DROP TABLE golconde.trigger_connections;

CREATE TABLE golconde.trigger_connections
(
  schema_name text NOT NULL DEFAULT 'public'::text, -- Name of the schema for the table the trigger is on.
  table_name text NOT NULL, -- Name of the table the trigger is on.
  connection_type text NOT NULL DEFAULT 'stomp'::text, -- Connection type, currently only stomp is supported.
  server_address inet NOT NULL DEFAULT '127.0.0.1'::inet, -- Message broker ip address
  server_port integer NOT NULL DEFAULT 61613, -- Port to connect to the message broker on.
  queue_name text NOT NULL, -- Destination queue name without queue prefix.  For /queue/test it would just be the value "test"
  primary_key text NOT NULL, -- JSON Encoded python list of primary key fields
  CONSTRAINT trigger_connections_pkey PRIMARY KEY (schema_name, table_name),
  CONSTRAINT trigger_connections_connection_type_check CHECK (connection_type = 'stomp'::text),
  CONSTRAINT trigger_connections_server_port_check CHECK (server_port > 0)
)
WITH (OIDS=FALSE);
GRANT SELECT ON TABLE golconde.trigger_connections TO public;
COMMENT ON COLUMN golconde.trigger_connections.schema_name IS 'Name of the schema for the table the trigger is on.';
COMMENT ON COLUMN golconde.trigger_connections.table_name IS 'Name of the table the trigger is on.';
COMMENT ON COLUMN golconde.trigger_connections.connection_type IS 'Connection type, currently only stomp is supported.';
COMMENT ON COLUMN golconde.trigger_connections.server_address IS 'Message broker ip address';
COMMENT ON COLUMN golconde.trigger_connections.server_port IS 'Port to connect to the message broker on.';
COMMENT ON COLUMN golconde.trigger_connections.queue_name IS 'Destination queue name without queue prefix.  For /queue/test it would just be the value "test"';
COMMENT ON COLUMN golconde.trigger_connections.primary_key IS 'JSON Encoded python list of primary key fields';

CREATE OR REPLACE FUNCTION golconde.trigger_function() RETURNS trigger AS
$$
# @summary Generic Golconde function enqueue data payloads
# @since 2009-02-09
# @author: Gavin M. Roy <gmr@myyearbook.com>
import json

sql = "SELECT * FROM golconde.trigger_connections WHERE schema_name = '%s' AND table_name = '%s'" % ( TD['table_schema'], TD['table_name'] )
result = plpy.execute(sql, 1)

if len(result) == 0:
  plpy.error('Missing golconde.trigger_connections data for %s.%s' % ( TD['table_schema'], TD['table_name'] ) )

if TD['event'] == 'INSERT':
  packet = {'action': 'add', 'data': TD['new']}
  
elif TD['event'] == 'DELETE':
  restriction = {}
  keys = json.loads(result[0]['primary_key'])
  for key in keys:
   restriction[key] = TD['old'][key]
  packet = {'action': 'delete', 'restriction': restriction}

elif TD['event'] == 'UPDATE':
  restriction = {}
  keys = json.loads(result[0]['primary_key'])
  for key in keys:
   restriction[key] = TD['old'][key]
  packet = {'action': 'update', 'restriction': restriction, 'data': TD['new']}

payload = json.dumps(packet)

# Enqueue it
if result[0]['connection_type'] == 'stomp':
  import stomp
  queue_name = '/queue/%s' % result[0]['queue_name']
  connection = stomp.Connection([(result[0]['server_address'], result[0]['server_port'])])
  connection.start()
  connection.connect()
  connection.send(destination=queue_name, message=payload)
else:
  plpy.error('golconde.trigger_function error: Invalid connection type specified "%s"' % result[0]['connection_type']) 

$$ LANGUAGE plpythonu;
GRANT EXECUTE ON FUNCTION golconde.trigger_function() TO public;
