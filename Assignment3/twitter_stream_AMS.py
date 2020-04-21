
from time import sleep
import tweepy
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from random import randint as randi
from random import shuffle
import json
import time


class MyStreamListener(tweepy.StreamListener):

	def __init__(self,time_limit):
		self.twitter_user = []
		self.count_user = []
		self.num_samples = 10000
		self.count = 0
		self.start_time = time.time()
		self.time_limit = time_limit*60
		super(MyStreamListener, self).__init__()
	

	def reservoir_sampling(self,uid):
		rnum = randi(1,self.count)
		if rnum <= self.num_samples:
			self.twitter_user[rnum-1] = uid
			self.count_user[rnum-1] = 1


	def find_uid(self,uid):
		for i in range(len(self.twitter_user)):
			if self.twitter_user[i] == uid:
				return i
		return -1

	def on_status(self, status):
		uid = status.user.id
		if self.count %1000 == 0:
			json.dump(self.twitter_user,open("twitter_user.json","w+"))
			json.dump(self.count_user,open("count_user.json","w+"))
			print(uid,self.count,len(self.twitter_user),sum(self.count_user))
		index = self.find_uid(uid)
		if index == -1:
			self.count += 1
			if len(self.twitter_user) < self.num_samples:
				self.twitter_user.append(uid)
				self.count_user.append(1)
			else:
				self.reservoir_sampling(uid)
		else:
			self.count_user[index] += 1 


		## condition to be checked for implementing the terminating condition based on timestamp
		if (time.time()-self.start_time) >=  self.time_limit:
			return False
		# if self.count >= 1000:
		# 	return False

		
	# want the stream to be connected even after getting disconnected
	def on_error(self,status_code):
		if status_code == 420:
			print("Error occured")
			return True




def publish_message(producer_instance, topic_name, key, value):
	try:
		key_bytes = bytes(key, encoding='utf-8')
		value_bytes = bytes(value, encoding='utf-8')
		producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
		producer_instance.flush()
		print('Message published successfully.')
	except Exception as ex:
		print('Exception in publishing message')
		print(str(ex))


def connect_kafka_producer():
	_producer = None
	try:
		_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
	except Exception as ex:
		print('Exception while connecting Kafka')
		print(str(ex))
	finally:
		return _producer


def AMS(observations,ams_num_indexes):
	indexes = [i for i in range(len(observations))]
	shuffle(indexes)
	if ams_num_indexes > len(observations):
		print("Error!!!! "+"Number of AMS Indexes should be less than number of observations i.e. 10000")
		return
	indexes = indexes[:ams_num_indexes]

	surprise_number = 0
	for ind in indexes:	
		value = observations[ind:].count(observations[ind])
		surprise_number += 2*value-1

	surprise_number = (len(observations)*surprise_number)/ams_num_indexes
	
	return surprise_number





if __name__ == '__main__':
	access_token = "1250874788098105345-YVdXH4X09xCPMgkWZOqfDdakWqyPi7"
	access_token_secret =  "o5WdZYooUYmIj58g9ctaGUgtkP9FjpBiWOaCYeUxPNU94"
	consumer_key = "IroA2TqJFfvU5FSnIoyzZ1eNA"
	consumer_secret = "lWNWRNHOESPf5ujrc8oqRIGNGMJhZII94uwlGZy6N8x7oriJRc"

	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	myStreamListener = MyStreamListener(2)
	myStream = tweepy.Stream(auth = auth, listener=myStreamListener)
	myStream.filter(track = "Covid-19",languages = ['en'])

	# print(len(myStreamListener.twitter_user),len(myStreamListener.count_user))
	users = myStreamListener.twitter_user
	counts = myStreamListener.count_user

	print("computing AMS")
	ams_num_indexes = 1000
	surprise_number = AMS(counts,ams_num_indexes)
	print("AMS number for the observation :- " + str(surprise_number))


	# #***CHECKING WHETHER IMPLEMENTATION OF AMS IS CORRECT OR NOT
	# count_dic = {}
	# for c in counts:
	# 	if c not in count_dic:
	# 		count_dic[c] = 1
	# 	else:
	# 		count_dic[c] += 1
	# values = list(count_dic.values())
	# actual_surprise_number = 0
	# for l in values:
	# 	actual_surprise_number += l*l

	# print("Actual AMS number for the observation :- " + str(actual_surprise_number))

	