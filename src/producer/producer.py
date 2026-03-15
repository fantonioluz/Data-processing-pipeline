# producer.py
from kafka import KafkaProducer
import json, random, time, uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='pipeline-kafka-kafka-bootstrap.ingestion:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUTOS = ['notebook', 'teclado', 'monitor', 'mouse']

while True:
    evento = {
        'event_id':   str(uuid.uuid4()),
        'produto':    random.choice(PRODUTOS),
        'preco':      random.choice([
                          round(random.uniform(50, 5000), 2),
                          None,        # dado corrompido
                          -1,          # valor inconsistente
                      ]),
        'quantidade': random.randint(1, 10),
        'data':       random.choice([
                          datetime.utcnow().strftime('%d/%m/%Y'),   # formato BR
                          datetime.utcnow().strftime('%Y-%m-%d'),   # formato ISO
                      ]),
        'email':      random.choice(['user@ok.com', 'invalido', '']),
    }
    producer.send('raw-events', evento)
    time.sleep(0.5)
