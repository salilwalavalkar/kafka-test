# Delete Topics
./kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka-test-101.*

# Create Topics
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-test-101
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic kafka-partitions-test-101
