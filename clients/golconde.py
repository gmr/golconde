#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
Golconde Client

Client for acting upon the Golconde queues for data distribution
'''
class GolcondeClient:

  # Connect to the ActiveMQ Server
  def Connect(self, host, port):
    # Connect to Stomp
    
  # Add to the Golconde Distribution Queue
  def Add(self, queue, action, data):
    # Add to the queue