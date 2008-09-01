CREATE OR REPLACE FUNCTION golconde.addStatement(IN statement text) RETURNS bool AS
$$
# -*- coding: utf-8 -*-

# @summary Golconde Function to insert statement into a queue - a simple test
# @since 2008-08-31
# @author: Gavin M. Roy <gmr@myyearbook.com>

import pyactivemq
from pyactivemq import ActiveMQConnectionFactory

# Get our connection string
result = plpy.execute("SELECT value FROM golconde.settings WHERE setting = 'mqConnectString';");

# Create our factory, connection, session, queue and producer
factory = ActiveMQConnectionFactory(result[0]["value"])
connection = factory.createConnection()
session = connection.createSession()
queue = session.createQueue('Golconde.test')
producer = session.createProducer(queue)

# Create the message and send it
message = session.createTextMessage()
message.text = statement
producer.send(message)

#Tidy up and return true
connection.close()
return 't';
$$ LANGUAGE plpythonu;
