#!/usr/bin/env python

""" 
Golconde Consumer Client

Listens to incoming commands, performing the appropriate queueing for subsequent actions.  

@author Gavin M. Roy <gmr@myyearbook.com>
@since 2008-12-14
"""

import optparse, stomp, sys, thread, time, yaml

# Golconde Destination Queue Listener
class GolcondeDestinationHandler(object):
'''
  The destination object should process inbound packets, going to the canonical source, validating success, then distributing to the target queues
'''
  def __init__(self,config):
    print 'Initialized'

  def on_error(self, headers, message):
    print 'received an error %s' % message

  def on_message(self, headers, message):
    print 'received a message %s' % message


def main():
  # Set our various display values for our Option Parser
  usage = "usage: %prog [options]"
  version = "%prog 0.1"
  description = "Golconde command line daemon to listen to top level Golconde destination queues"

  # Create our parser and setup our command line options
  parser = optparse.OptionParser(usage=usage,version=version,description=description)

  parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=True,
                  help="make lots of noise [default]")
  parser.add_option("-c", "--config", 
                  action="store", type="string", default="golconde.yaml", 
                  help="Specify the configuration file to load.")
                  
  options, args = parser.parse_args()

  # Load the Configuration file
  try:
    stream = file(options.config, 'r')
    config = yaml.load(stream)
  except:
    print "Invalid or missing configuration file: %s" % options.config
    sys.exit(1)
  
  
  # Empty destination connection list
  destinationConnections = []
  
  # Loop through the destinationsy
  for (destination, targets) in config['Destinations'].items():
    print 'Subscribing to Destination "%s" on queue: %s' % (destination, targets['queue'])
    
    # Connect to our stomplistener forthe Destination
    stompConnection = stomp.Connection()
    stompConnection.add_listener(GolcondeDestinationHandler(targets))
    stompConnection.start()
    stompConnection.connect()
    stompConnection.subscribe(destination=targets['queue'],ack='auto')
  
  while (1):
    time.sleep(2)
  
if __name__ == "__main__":
	main()