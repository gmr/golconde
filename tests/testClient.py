#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
Goconde Test Client

Generate random Golconde Queue Messages for testing the various states and actions, for use with the base test suite

@since 2008-12-29
@author Gavin M. Roy <gmr@myyearbook.com>
'''

# Default Actions
actions = ['add','upsert','update','delete']

# Number of messages to limit to
limit = 5000

# Min User ID
min = 0

# Max User ID
max = 500

# ActiveMQ Server & Port
server = '127.0.0.1'
port = 61613

# Define the queue
queue = '/queue/golconde.test'

import random, stomp, sys, simplejson as json

def main():

  # Connect to our Stomp Connection
  connection = stomp.Connection([(server, port)])
  connection.start()
  connection.connect()

  # Empty list
  r = []

  # Loop through the number of operations to perform
  for i in range(0, limit):

    # Randomize our seed
    random.seed()
    
    # Pick our random ID Values
    valueA = random.randint(min,max)
    valueB = random.randint(min,max)

    # Randomly pick an action based upon if we've seen this item or not
    if (valueA,valueB) in r or (valueB,valueA) in r:
      a = random.randint(2,3)
      # If it's a delete, remove it from the stack so we can act on it again
      if actions[a] == 'delete':
        if (valueA,valueB) in r:
          r.remove((valueA,valueB))
        else:
          r.remove((valueB,valueA))
    else:
      r.append((valueA,valueB))
      a = random.randint(0,1)

    statement = json.dumps({'action': actions[a], 'data': {'user_id': valueA, 'friend_id': valueB}})
    connection.send(destination=queue, message=statement)
    
  print '%i distinct combinations inserted with %i actions' % ( len(r), limit)

if __name__ == '__main__':
  main()