#!/usr/bin/env python

""" 
Golconde Consumer Client

Listens to incoming commands, performing the appropriate queueing for subsequent actions.  

@author Gavin M. Roy <gmr@myyearbook.com>
@since 2008-12-14
"""

import optparse, stomp, sys, time, yaml

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
  
  for (destination, targets) in config['Destinations'].items():
    print 'Subscribing to Destination: %s' % destination
    for (target, config) in targets.items():
      print 'Connecting to Target: %s' % target
      for (key,value) in config.items():
        print 'Set %s to %s' % ( key, value)  
  
if __name__ == "__main__":
	main()