 # Kafka + inventory topic
docker compose up -d               


uv run consumer.py worker -l warn        
uv run producer.py                        
curl localhost:8080/produce               
To test without waiting 5 minutes, start the consumer with WINDOW_SECONDS=20 uv run consumer.py worker -l warn.
