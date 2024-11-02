import random
import time
from kafka import KafkaProducer
import json

# Configuración del productor de Kafka
producer = KafkaProducer(bootstrap_servers='lab9.alumchat.lol:9092')

# Mapa de direcciones del viento a 3 bits
wind_direction_map = {'N': 0, 'NE': 1, 'E': 2, 'SE': 3, 'S': 4, 'SO': 5, 'O': 6, 'NO': 7}
wind_directions = list(wind_direction_map.keys())

# Funciones de codificación y decodificación
def encode_data(temperature, humidity, wind_direction):
    temp_scaled = int(temperature * (16383 / 110))  # Escala temperatura a 14 bits
    hum_scaled = humidity  # Humedad en 7 bits
    wind_scaled = wind_direction_map[wind_direction]  # Dirección de viento en 3 bits
    
    # Combina los valores en un solo entero de 24 bits
    encoded_data = (temp_scaled << 10) | (hum_scaled << 3) | wind_scaled
    return encoded_data.to_bytes(3, 'big')  # Convierte a 3 bytes

def generate_sensor_data():
    # Genera datos de los sensores
    temperature = random.gauss(55, 15)  # Distribución normal con media 55 y desviación 15
    temperature = min(max(temperature, 0), 110)  # Asegura que esté en el rango [0, 110]
    humidity = random.randint(0, 100)  # Humedad relativa en porcentaje
    wind_direction = random.choice(wind_directions)  # Dirección del viento
    return temperature, humidity, wind_direction

topic = "21527"  

# Ciclo de envío de datos
i = 0
while i < 100:
    # Genera y codifica los datos
    temperature, humidity, wind_direction = generate_sensor_data()
    encoded_message = encode_data(temperature, humidity, wind_direction)
    
    # Envía los datos codificados a Kafka
    producer.send(topic, encoded_message)
    print(f"Sent encoded message: {encoded_message}")
    
    #
    time.sleep(0.5)
    i+=1
