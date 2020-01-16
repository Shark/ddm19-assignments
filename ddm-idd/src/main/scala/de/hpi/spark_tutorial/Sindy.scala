package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.Count

object Sindy extends App {

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    println("---------------------------------------------------------------------------------------------------------")
    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

        val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
          .map(name => s"data/TPCH/tpch_$name.csv")

//    val inputs = List("region", "nation")
//      .map(name => s"data/test/tpch_$name.csv")


    time {
      discoverINDs(inputs, spark)
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println(s"Execution: ${t1 - t0} ms")
    result
  }

  def preaggregation(frame: Dataset[(String, String)], spark: SparkSession): Dataset[(String, Seq[String])] = {
    import spark.implicits._

    frame
      .groupByKey(p => p._1)
      .mapValues { case (key, values) => Set(values) }
      .reduceGroups((storage, value) => storage ++ value)
      .map { case (key, value) => (key, value.seq.toList.distinct) }
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types

    val frames = inputs.map(path => spark.read
      .option("sep", ";")
      .option("header", "true")
      //      .option("inferSchema", "true")
      .csv(path)
    )
    val tuples_frames = frames.map(frame => {
      val columns = frame.columns
      val cells = frame.flatMap(row => row.toSeq.map(p => String.valueOf(p)).zip(columns))
      preaggregation(cells, spark)
    })


    val cache_based_preaggregation = tuples_frames.reduce((acc, frame) => acc.union(frame))
    //    System.out.println(cache_based_preaggregation.count())
//    cache_based_preaggregation.show()
    // TODO ADD partitioning by hash
    //    cache_based_preaggregation.groupBy
    val attribute_sets = cache_based_preaggregation.groupByKey(_._1).mapValues({ case (key, value) => value }).reduceGroups((acc, value) => acc ++ value).map((value) => value._2)
    val inclusion_list = attribute_sets.flatMap((set) => {
      set.map(e => (e, set.filter(el => !el.equals(e))))
    })

    val aggregate = inclusion_list.groupByKey(_._1).reduceGroups((acc, value) => (acc._1, acc._2.intersect(value._2))).map(p => p._2)
    val results = aggregate.filter(data => data._2.length > 0)
    val sorted = results.sort("_1")
    sorted.collect().foreach(p => printf("%s < %s\n", p._1, p._2.sorted.mkString(", ")))


    cache_based_preaggregation

    // TODO
  }
}