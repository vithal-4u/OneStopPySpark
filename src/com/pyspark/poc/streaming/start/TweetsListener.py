'''
Created on 10-Jun-2020

Basic operation on DataFrames

@author: kasho
'''

import json
from tweepy.streaming import StreamListener

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'])
            self.client_socket.send(msg['text'].encode('utf-8'))
            #print(data.split('\n'))
            #print(data.encode('utf-8'))
            #self.client_socket.send(data.encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True
            

