docker compose up -d                       # Kafka + inventory topic
uv run consumer.py worker -l warn          # terminal 1
uv run producer.py                         # terminal 2
curl localhost:8080/produce                # each hit = 1 event
To test without waiting 5 minutes, start the consumer with WINDOW_SECONDS=20 uv run consumer.py worker -l warn.