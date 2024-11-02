import random
import json
import numpy as np


from kafka import KafkaProducer
import time
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

def generar_datos_sensor():
    temperatura = round(np.random.normal(55, 15), 2)  # Media 55, varianza 15
    humedad = int(np.random.normal(55, 15))  # Media 55, varianza 15, como entero
    direccion_viento = random.choice(["N", "NO", "O", "SO", "S", "SE", "E", "NE"])
    
    datos = {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }
    return json.dumps(datos)


producer = KafkaProducer(bootstrap_servers='164.92.76.15:9092')
topic = '21527' 

for i in range(0,100):
    data = generar_datos_sensor()
    producer.send(topic, key=b'sensor1', value=data.encode('utf-8'))
    print(f"Data sent: {data}")
    time.sleep(1)