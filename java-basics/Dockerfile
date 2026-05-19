FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY KafkaTradeConsumer.class /app/
COPY kafka-clients-3.6.0.jar /app/
COPY slf4j-api-2.0.7.jar /app/
COPY slf4j-simple-2.0.7.jar /app/
CMD java -cp "kafka-clients-3.6.0.jar:slf4j-api-2.0.7.jar:slf4j-simple-2.0.7.jar:." KafkaTradeConsumer
