'''
Created on 10-Jun-2020

Basic operation on DataFrames

@author: kasho
'''

import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from com.pyspark.poc.streaming.start.TweetsListener import TweetsListener
import socket

#listener = TweetsListener()

# Set up your credentials from http://apps.twitter.com
consumer_key = 'RaLDvaHcsRlHoiPKRrXYiMK8n'
consumer_secret = 'o6ldjGvEBHZdv2rjp3vZdWqfWjTxA9SKUmiUjgZpyD3si2c804'
access_token = '811420957-FQqylhzUqZHQqtfsH3PepQPhuotTN2eyCUDRkBzt'
access_secret = 'R58HTu1mpl2M1Z8ebVEGDKbiNukYcGVKpMngGeboGicRq'


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['football'])

if __name__ == "__main__":
    s = socket.socket()
    host = "localhost"
    port = 9009
    s.bind((host, port))
    print("Listening on port: %s" % str(port))

    s.listen(5)
    c, addr = s.accept()
    print("Received request from: " + str(addr))

    sendData(c)


