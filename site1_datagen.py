from __future__ import absolute_import

from mqtt import MqttPublishHandler
import sys
import time
import csv
import os
import schedule


line = []

line.append("car_make")
line.append("car_model")
line.append("car_model")
line.append("engine_on")
line.append("otonomo_id")
line.append("geocoding_city")
line.append("time")
line.append("latitude")
line.append("longitude")
line.append("heading")
line.append("speed")
line.append("provider")
line.append("geocoding_country_long")
line.append("geocoding_state")
line.append("geocoding_town")
line.append("ignition")
line.append("geocoding_suburb")
line.append("geocoding_road")


# mqtt_address = str(sys.argv[1])
# runtime = str(sys.argv[2])
# gen_rate = str(sys.argv[3])

run_at = str(sys.argv[1])
mqtt_address = "127.0.0.1"
t1 = "T-1"
t2 = "T-2"
t3 = "T-3"
gen_rate = "10"
runtime = "600"

def job():
    mqttph1 = MqttPublishHandler(mqtt_address, t1, 'mqtt-pub', 'mqtt-pub') #host, client id, username & password
    mqttph1.connect()
    mqttph2 = MqttPublishHandler(mqtt_address, t2, 'mqtt-pub', 'mqtt-pub') #host, client id, username & password
    mqttph2.connect()
    mqttph3 = MqttPublishHandler(mqtt_address, t3, 'mqtt-pub', 'mqtt-pub') #host, client id, username & password
    mqttph3.connect()

    gen_irate = int(gen_rate)
    iruntime = int(runtime)

    end = time.time() + iruntime
    while time.time() < end:
        message = "sda" + ":" + line[12] + ":" + str(time.time()) + ":" + line[6] + ":" + line[7] + ":" + line[0] + ":" + \
                  line[1] + ":" + line[2] \
                  + ":" + line[3] + ":" + line[4] + ":" + line[5] + ":" + line[2] + ":" + line[8] + ":" + line[8] + ":" + \
                  line[10] + ":" + line[11] \
                  + ":" + line[13] + ":" + line[14] + ":" + line[15] + ":" + line[16]
        #message1 = "time:"+ str(time.time()) + ":"+ t1
        #message2 = "time:" + str(time.time()) + ":" + t2
        #message3 = "time:" + str(time.time()) + ":" + t3
        time.sleep(gen_irate/1000) #ms
        #print(message1)
        #print(message2)
        #print(message3)
        mqttph1.publish(t1, message)
        mqttph2.publish(t2, message)
        mqttph3.publish(t3, message)

    mqttph1.disconnect()
    mqttph2.disconnect()
    mqttph3.disconnect()
    return schedule.CancelJob

schedule.every().day.at(run_at).do(job)

while True:
    schedule.run_pending()
    time.sleep(1)