# This application records MQTT payloads in csv file.
# Authors: Ahmed MJADI

import paho.mqtt.client as mqtt
import json
import csv
from datetime import datetime


  
#Variables
	#CSV Variables
header = ['time','scan_time', 'anchor_id', 'tag_id', 'Temperature', 'Humidity', 'rssi']
secondheader = ['Time', 'Topic', 'Payload']

	#Broker Variables
Broker_Adress = "mqtt.hardiot.com"
Broker_Port = 1883
Keep_Alive = 60
Broker_Topic = "NXTBLE/devices/+/telemetry"   #   NXTBLE/Technopark/84CCA8550D40
data = {}

	#Things board variables 
# Broker_Adress = "demo.thingsboard.io"
# broker_user = "ahmedmjaadi"
# broker_password = "ahmedmjaadi"
# Broker_Port = 1883
# Keep_Alive = 60
# Broker_Topic = "NXTBLE/devices/+/telemetry"
# data = {}

   

# Functions

def createCSVLogs():
	with open('Mqtt_Json_Logs.csv', 'a+', encoding='UTF8', newline='') as f:
		writer = csv.writer(f)
		# write the header
		writer.writerow(header)
		# close the file
		f.close()

	with open('MqttLogger.csv', 'a+', encoding='UTF8', newline='') as f:
		writer = csv.writer(f)
		# write the header
		writer.writerow(secondheader)
		# close the file
		f.close()


def onConnect(client, userdata, flags, rc):
	print("[i] MQTT Client Connected")
	client.subscribe(Broker_Topic)
	print("[i] MQTT Client Sybscribed")

def onDisconnect(client, userdata, rc):
	print("[E] Client disconnected")
	# client.on_connect = onConnect


def onMessage(client, userdata, msg):
	try:
		print("[i] GOT Payload: ")	
		print("[D] Topic: " + msg.topic + ", Payload : " + str(msg.payload))

		#LOG the MSG:
		timeStamp = datetime.now()
		# MSGZ=[str(data_buffer)]
		ROWsi=[timeStamp,msg.topic, msg.payload]

		with open('MqttLogger.csv', 'a+', encoding='UTF8', newline='') as f:
			writerL = csv.writer(f)

			# print("[D] Writing on csv file !")
			# write the data
			writerL.writerow(ROWsi)
			# close the file
			f.close()
		# Handle data	to be uncommented
		handle_data(msg.payload)
	except Exception as e:
		print("[E]")
		print(e)

def handle_data(msg):
	try:
		# todo: check msg before load
		data_buffer = json.loads(msg)
		A_ID = data_buffer.get('anchor_id')
		A_time = data_buffer.get('time')
		tags = data_buffer['tags']
		for BLE_dataL in tags:	
			Tg_ID = BLE_dataL['ble_id']
			T = BLE_dataL['T']
			H = BLE_dataL['H']
			rssi = BLE_dataL['rssi']
		#logging
		timeStamp = datetime.now()

		# MSGZ=[str(data_buffer)]
		ROWz=[timeStamp,A_time ,A_ID, Tg_ID,T,H,rssi]

		with open('Mqtt_Json_Logs.csv', 'a+', encoding='UTF8', newline='') as f:
			writerR = csv.writer(f)

			print("[D] Writing on csv file !")
			# write the data
			writerR.writerow(ROWz)
			# close the file
			f.close()
	except Exception as e:
		print("[E]")
		print(e)

# 	# check values
# 	if((n is not None) & (a is not None) & (rssi is not None)):
# 		print(n)
# 		print(a)
# 		print(rssi)
		
# 	else:
# 		print("Error")
	

	
#App:    
	
createCSVLogs()
print("MQTT Logger to CSV\nVersion: 1.0    \n#Dev. by Ahmed MJADI")
client = mqtt.Client("", True, None, mqtt.MQTTv31)
client.on_connect = onConnect
client.on_message = onMessage
client.on_disconnect = onDisconnect
# client.username_pw_set("ahmedmjaadi","ahmedmjaadi") # Authentification for thingsboard
client.connect(Broker_Adress, Broker_Port, Keep_Alive)
client.loop_forever()
