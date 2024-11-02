from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    '21527', 
    bootstrap_servers='lab9.alumchat.lol:9092',
    group_id='foo2',
    auto_offset_reset='earliest'
)

# Mapa de bits a direcciones de viento
wind_direction_map = {0: 'N', 1: 'NE', 2: 'E', 3: 'SE', 4: 'S', 5: 'SO', 6: 'O', 7: 'NO'}

def decode_data(encoded_bytes):
    encoded_data = int.from_bytes(encoded_bytes, 'big')
    
    # Extrae los valores originales
    temp_scaled = (encoded_data >> 10) & 0x3FFF  # 14 bits para temperatura
    hum_scaled = (encoded_data >> 3) & 0x7F      # 7 bits para humedad
    wind_scaled = encoded_data & 0x7             # 3 bits para dirección del viento
    
    # Escala la temperatura al rango original
    temperature = temp_scaled * (110 / 16383)
    humidity = hum_scaled
    wind_direction = wind_direction_map[wind_scaled]
    
    return {
        "temperatura": round(temperature, 2),
        "humedad": humidity,
        "direccion_viento": wind_direction
    }

# Variables para graficación
all_temp = []
all_hume = []
all_wind = []

# Configuración del gráfico en vivo
plt.ion()
fig, ax = plt.subplots(2, 1, figsize=(10, 8))

while True:
    # Consume y procesa cada mensaje
    for message in consumer:
        payload = decode_data(message.value)
        
        # Añade datos a las listas
        all_temp.append(payload['temperatura'])
        all_hume.append(payload['humedad'])
        all_wind.append(payload['direccion_viento'])
        
        # Graficación en tiempo real
        ax[0].cla()
        ax[1].cla()
        ax[0].plot(all_temp, label="Temperatura (°C)" ,color="blue")
        ax[1].plot(all_hume, label="Humedad (%)" ,color="red")
        ax[0].set_ylabel("Temperatura (°C)")
        ax[1].set_ylabel("Humedad (%)")
        ax[0].legend(loc="upper right")
        ax[1].legend(loc="upper right")
        
        plt.draw()
        plt.pause(0.1)
