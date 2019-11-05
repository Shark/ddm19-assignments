FROM maven:3-jdk-8-slim
WORKDIR /usr/src/app
COPY pom.xml /usr/src/app/
RUN mvn dependency:resolve-plugins dependency:go-offline -B
COPY . /usr/src/app
RUN mvn install
ENTRYPOINT ["/usr/local/openjdk-8/bin/java", "-jar", "/usr/src/app/target/ddm-pc-1.0.jar"]
