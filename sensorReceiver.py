from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

consumer = KafkaConsumer(
    '21527',
    group_id='grupo1',
    bootstrap_servers='164.92.76.15:9092'
)

all_temp, all_hume, all_wind = [], [], []

plt.ion()  # Habilitar modo interactivo para actualización en vivo
fig, (ax1, ax2) = plt.subplots(2, 1)

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    all_temp.append(data['temperatura'])
    all_hume.append(data['humedad'])
    all_wind.append(data['direccion_viento'])

    # Graficar
    ax1.clear()
    ax2.clear()
    ax1.plot(all_temp, label="Temperatura (°C)", color='red')
    ax2.plot(all_hume, label="Humedad (%)", color='blue')
    ax1.legend()
    ax2.legend()
    plt.draw()
    plt.pause(0.1)
