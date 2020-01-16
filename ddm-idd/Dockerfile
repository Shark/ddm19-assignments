FROM maven:3-jdk-8-slim AS builder
WORKDIR /usr/src/app
COPY pom.xml /usr/src/app/
RUN mvn dependency:resolve-plugins dependency:go-offline -B
COPY . /usr/src/app
RUN mvn package

FROM bde2020/spark-submit
RUN mkdir /app
COPY data /app/
COPY --from=builder /usr/src/app/target/SparkTutorial-1.0.jar /app/app.jar
CMD ["/spark/bin/spark-submit", "--class", "de.hpi.spark_tutorial.SimpleSpark", "--master", "local[8]", "--deploy-mode", "client", "--total-executor-cores", "2", "/app/app.jar"]