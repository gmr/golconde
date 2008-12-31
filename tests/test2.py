#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
Goconde Test Client

This is only to do additions and sets to make sure surrogate key behaviors are correct

@since 2008-12-31
@author Gavin M. Roy <gmr@myyearbook.com>
'''

# Default Actions
actions = ['add','set','update','delete']

# Default Lorem
lorum = [
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin ut orci. Curabitur et odio et lectus interdum pulvinar. Curabitur sed dui. Donec interdum pretium quam. Nam mattis metus sit amet orci. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum lobortis pulvinar magna. Proin eget ligula eget felis placerat tincidunt. Maecenas vitae est at nibh egestas vestibulum. In lorem. Donec eleifend magna ut est. In aliquam ante et nisl. Aliquam tempus dignissim est. Nulla aliquet felis a velit porttitor fringilla. Vivamus sit amet felis. Etiam congue lacus a nisl. Praesent a pede. Aenean faucibus, nulla nec pretium interdum, risus felis iaculis tortor, non tincidunt urna elit sed turpis.",
  "In pulvinar blandit arcu. Sed tellus erat, sodales quis, bibendum sed, consequat nec, sapien. Fusce tortor. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed sed dolor. Nulla in magna a sapien ultricies ullamcorper. Curabitur vehicula, risus quis bibendum lobortis, lorem metus dictum felis, ac egestas eros massa in mi. Nullam malesuada quam et risus. Phasellus pede. Cras diam.",
  "Suspendisse ut mi. Nunc nulla. In arcu nibh, consectetur et, dignissim consequat, blandit vitae, felis. Pellentesque dolor nulla, accumsan eu, pellentesque ac, mattis id, massa. Nunc sem. Quisque at magna. Aliquam scelerisque justo a dui. Aliquam nisl diam, ultricies sed, dapibus pulvinar, pulvinar quis, nunc. Morbi dapibus suscipit felis. Maecenas tempor pharetra ante. Nunc pulvinar sapien vel nisi. Etiam porttitor sem eu justo volutpat viverra. Aenean a orci. Nullam sagittis aliquet justo. Vestibulum eget nisl.",
  "Suspendisse facilisis viverra turpis. Mauris tortor. Cras lacus tellus, accumsan non, dapibus vel, aliquam sit amet, enim. In tincidunt urna ac magna. Integer vitae erat. Phasellus sed nisl. Integer ut metus eu nisl hendrerit tincidunt. Sed eu lorem quis velit tempus sodales. Etiam id tortor. Suspendisse massa. Duis a nisl. In erat. Pellentesque ut est. Pellentesque augue. Ut non magna. Duis eu purus vitae mauris dapibus mattis. Donec interdum.",
  "Curabitur ullamcorper urna accumsan dolor. Sed augue. Nullam massa. Morbi lobortis. Maecenas egestas mauris in arcu placerat elementum. Curabitur interdum porttitor metus. Curabitur nunc orci, mattis eget, laoreet quis, euismod vel, libero. Phasellus ac est a eros viverra viverra. In quis odio at elit tempor vehicula. Nunc lobortis interdum urna."
]

# Number of messages to limit to
limit = 100

# ActiveMQ Server & Port
server = '127.0.0.1'
port = 61613

# Define the queue
queue = '/queue/golconde.test2'

# Imports
import random, simplejson as json, stomp, time

# Main function for command line execution
def main():

  # Connect to our Stomp Connection (@todo move to Golconde Client)
  connection = stomp.Connection([(server, port)])
  connection.start()
  connection.connect()

  # Loop through the number of operations to perform
  for i in range(0, limit):

    # Randomize our seed
    random.seed()

      # Build our Golconde Message, we should replace this with passing in the dictionary to a Golconde client function
    statement = json.dumps({'action': actions[random.randint(0,1)], 'data': {'description': lorum[random.randint(0,4)], 'added_at': time.asctime()}})
    
    # Send the statement via Stomp, we should replace this to make it internal to the Golconde client function
    print 'Sending %s' % statement
    connection.send(destination=queue, message=statement)
    
  print '%i distinct combinations inserted with %i actions' % ( len(r), limit)

if __name__ == '__main__':
  main()