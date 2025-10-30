# kafka_producer_sim.py
# Simulador de datos para topic Kafka - Casos COVID-19
# Autor: Emerson Manuel Basilio Navarro
# Curso: Big Data y Análisis de Datos Masivos - UNAD
# Año: 2025

from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulación de nuevos casos
datos = [
    {"id_de_caso": "1", "departamento_nom": "Antioquia", "ciudad_municipio_nom": "Medellín", "sexo": "F", "fuente_tipo_contagio": "Comunitario", "estado": "Leve"},
    {"id_de_caso": "2", "departamento_nom": "Valle del Cauca", "ciudad_municipio_nom": "Cali", "sexo": "M", "fuente_tipo_contagio": "Relacionado", "estado": "Recuperado"},
    {"id_de_caso": "3", "departamento_nom": "Bogotá", "ciudad_municipio_nom": "Bogotá D.C.", "sexo": "F", "fuente_tipo_contagio": "Importado", "estado": "Fallecido"}
]

for caso in datos:
    producer.send('covid_topic', caso)
    print(f"📤 Enviado a Kafka: {caso}")
    time.sleep(3)

producer.flush()
print("✅ Simulación completada.")
