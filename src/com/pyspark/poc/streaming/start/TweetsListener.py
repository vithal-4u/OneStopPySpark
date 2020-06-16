'''
Created on 16-Jun-2020

This file will listen to the twitter app for new and old tweets based on the
    twitter stream filer we applied like twitter_stream.filter(track=['trump']).
    All the tweets related to Trump word will be fetched from twitter via over
    created app.

    After reading each line we are binding the line to the port mention in the
    TweetRead.py file.

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
            #self.client_socket.send(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True
            

