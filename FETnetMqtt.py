# coding:utf-8
import codecs
import json
import ssl
import paho.mqtt.client as mqtt
from apscheduler.schedulers.blocking import BlockingScheduler
import time
from datetime import datetime
import random

token = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
token[0] = '4ffae3450d3e4f8dba2157b9140f60bf'
token[1] = '820dbb922c5b426289d8ed04f96de9ba'
token[2] = '1dfd60cc47c84ccd8a6f30a39f52d00f'
token[3] = '34b2df3fc7f74a87b48767d62f07441c'
token[4] = '2e6fc337caa84d9c92ab356d97dcdbd2'
token[5] = 'cc37b15ad8854c7bb3a15d09157f22d4'
token[6] = '7431d52f0125450dac83a2b644437e96'
token[7] = '35b28cd727ba40d7b385d38a2e2e4970'
token[8] = 'dce948493d9d4b8ebf588b441f4fc927'
token[9] = '2d56fd12b07940178152db9896861167'
token[10] = '2862fbd144bc42849395bb43b3ac8256'
token[11] = '78b77b128a0f47ab8709d877a0ba3c0c'
token[12] = '02a1b3c9f7a145a0af446e629ba4605d'
token[13] = '9489d6bc52594b19ad70c62df2b83488'
token[14] = '86ac502bd2bc4319a2f893864d9a8245'


def PowerMeter():
    MainPW_meter = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    MainPW_meter[0] =  round(random.uniform(59,61),2)
    MainPW_meter[1] =  round(random.uniform(370,380),2)
    MainPW_meter[2] =  round(random.uniform(370,380),2)
    MainPW_meter[3] =  round(random.uniform(370,380),2)
    MainPW_meter[4] =  round(random.uniform(370,380),2)
    MainPW_meter[5] =  round(random.uniform(20,30),2)
    MainPW_meter[6] =  round(random.uniform(20,30),2)
    MainPW_meter[7] =  round(random.uniform(20,30),2)
    MainPW_meter[8] =  round(random.uniform(20,30),2)
    MainPW_meter[9] =  round(random.uniform(30,50),2)
    MainPW_meter[10] =  round(random.uniform(5,10),2)
    MainPW_meter[11] =  round(random.uniform(35,60),2)
    MainPW_meter[12] =  round(random.uniform(0.5,0.9),2)
    MainPW_meter[13] =  round(random.uniform(5,10),2)
    MainPW_meter[14] =  round(random.uniform(35,60),2)
    MainPW_meter[15] =  round(random.uniform(3,5),2)
    MainPW_meter[16] =  1
    
    return (MainPW_meter)

def aligned_time():
    now = datetime.now()
    aligned_minute = now.minute - (now.minute % 5)
    #aligned_second = now.second - (now.second % 5)
    aligned_time = now.replace(minute=aligned_minute, second=0, microsecond=0)
    print(aligned_time)
    #aligned_time = now.replace(minute=aligned_second, microsecond=0)
    timestamp = int(time.mktime(aligned_time.timetuple()))
    print(aligned_time)
    return timestamp

def send_data():
    timestamp = aligned_time()
    for i in range(15):
        access_token = token[i]
        Meter_data = PowerMeter()
        FET_Publish_Station(Meter_data,access_token,timestamp)

def FET_Publish_Station(Meter_data,access_token,timestamp):
    try:
        
        client_sta = mqtt.Client('', True, None, mqtt.MQTTv31)
        client_sta.username_pw_set('infilinkems', 'xFqYswBn')
        client_sta.tls_set(cert_reqs=ssl.CERT_NONE)
        client_sta.connect('mqtt-device.fetiot3s1.fetnet.net', 8884 , 60)
        client_sta.loop_start()
        time.sleep(2)
        client_sta.on_connect
        mod_payload = [
            {"access_token":access_token,
            "app":"ShangriLa2024TPE",
            "type":"electricity_meter",
            "data":[
                {"timestemp":timestamp,
                "values":{
                    "voltage_r_s":Meter_data[1],
                    "voltage_s_t":Meter_data[2],
                    "voltage_t_r":Meter_data[3],
                    "voltage_line_avg":Meter_data[4],
                    "current_r":Meter_data[5],
                    "current_s":Meter_data[6],
                    "current_t":Meter_data[7],
                    "current_phase_avg":Meter_data[8],
                    "frequency":Meter_data[0],
                    "power": Meter_data[9],
                    "power_kvar":Meter_data[10],
                    "energy":Meter_data[14],
                    "immediate_demand":Meter_data[13],
                    "pf":Meter_data[12],
                    "alive":Meter_data[16],
                    "type":"三相三線"
                    }}]}
            ] 
        #time.sleep(2)
        print (mod_payload)
        data03 = client_sta.publish('/INFILINKEMS/v1/telemetry/infilink',json.dumps(mod_payload))
        time.sleep(5)
        print(data03)


    except:
        pass
    
def MQTT_Connect_sta():
    try:
        global client_sta
        client_sta = mqtt.Client('', True, None, mqtt.MQTTv31)
        client_sta.username_pw_set('infilink_ShangriLa2024TPE', 'wCGTd25n')
        client_sta.tls_set(cert_reqs=ssl.CERT_NONE)
        client_sta.connect('mqtt-device.fetiot3s1.fetnet.net', 8884 , 60)
        client_sta.loop_start()
        time.sleep(2)
        client_sta.on_connect
    except:
        print("error_connect_Sta")


    
scheduler = BlockingScheduler()
#scheduler.add_job(send_data, 'interval', minutes=5)
scheduler.add_job(send_data, 'interval', seconds=10)


try:
    scheduler.start()
except:
    pass
