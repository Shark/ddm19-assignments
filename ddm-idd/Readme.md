This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit.
Things to do after checking out and opening in intellij:
    Add scala SDK 2.11.11 (tested with this version, it has to be 2.11.x since this is the spark version)
    Add jdk 8
    add Framework support of Maven
To build the project:
    run mvn package
    
docker run -it --rm -v $(pwd):/app maven:3-jdk-8-slim bash
mvn package

docker run -it --rm -v $(pwd):/app bde2020/spark-submit bash
/spark/bin/spark-submit --class de.hpi.spark_tutorial.SimpleSpark --master local[8] --deploy-mode client --executor-memory 2G --total-executor-cores 2 target/SparkTutorial-1.0.jar