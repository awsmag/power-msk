# create topic
podman exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create  --topic test

# produce message
podman exec -it kafka \
  kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic test