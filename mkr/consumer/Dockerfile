FROM eclipse-temurin:21-jdk

WORKDIR /app

RUN mkdir -p lib

ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar lib/kafka-clients-3.7.0.jar
ADD https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.13/slf4j-api-2.0.13.jar lib/slf4j-api-2.0.13.jar
ADD https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.13/slf4j-simple-2.0.13.jar lib/slf4j-simple-2.0.13.jar

COPY src/main/java/ConsumerApp.java .

RUN javac -cp "lib/*" ConsumerApp.java

CMD ["java", "-cp", ".:lib/*", "ConsumerApp"]