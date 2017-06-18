# harald-analytics

Install Apache ZooKeeper (MacOS)

    brew update
    brew install zookeeper

Install Apache Kafka (MacOS)

    brew install kafka

To have launchd start kafka now and restart at login:

    brew services start kafka

Or, if you don't want/need a background service you can just run:

    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties

Bluemix details

https://blog.altoros.com/using-spark-streaming-apache-kafka-and-object-storage-for-stream-processing-on-bluemix.html