package com.labs1904.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes


case class KafkaData(marketplace: String,
                     customer_id: String,
                     review_id: String,
                     product_id: String,
                     product_parent: String,
                     product_title: String,
                     product_category: String,
                     star_rating: String,
                     helpful_votes: String,
                     total_votes: String,
                     vine: String,
                     verified_purchase: String,
                     review_headline: String,
                     review_body: String,
                     review_date: String)

case class HBasepostMerge(name: String,
                          username: String,
                          email: String,
                          birthdate: String,
                          sex: String,
                          marketplace: String,
                          customer_id: String,
                          review_id: String,
                          product_id: String,
                          product_parent: String,
                          product_title: String,
                          product_category: String,
                          star_rating: String,
                          helpful_votes: String,
                          total_votes: String,
                          vine: String,
                          verified_purchase: String,
                          review_headline: String,
                          review_body: String,
                          review_date: String
                         )

/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder().config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.fs.defaultFS", "hdfs://manager.hourswith.expert:8020").appName(jobName).master("local[*]").getOrCreate()
      val bootstrapServers = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"

      import spark.implicits._
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews-as-tabs")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      df.printSchema()

      val reviews = df.map(line => {
        val arr = line.split("\t")
        KafkaData(arr(0),
          arr(1),
          arr(2),
          arr(3),
          arr(4),
          arr(5),
          arr(6),
          arr(7),
          arr(8),
          arr(9),
          arr(10),
          arr(11),
          arr(12),
          arr(13),
          arr(14))
      })


      val customerData = reviews.mapPartitions(partition => {
        var connection: Connection = null
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
        connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("shared:users"))

        val iter = partition.map(eachKafkaRow => {
          val get = new Get(Bytes.toBytes(eachKafkaRow.customer_id)).addFamily(Bytes.toBytes("f1"))
          val result = table.get(get)

          val email =
            Bytes.toString(
              result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
            )
          val name =
            Bytes.toString(
              result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"))
            )
          val username =
            Bytes.toString(
              result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("username"))
            )
          val sex =
            Bytes.toString(
              result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex"))
            )
          val birthdate =
            Bytes.toString(
              result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate"))
            )

          HBasepostMerge(
            name = name,
            username = username,
            email = email,
            birthdate = birthdate,
            sex = sex,
            marketplace = eachKafkaRow.marketplace,
            customer_id = eachKafkaRow.customer_id,
            review_id = eachKafkaRow.review_id,
            product_id = eachKafkaRow.product_id,
            product_parent = eachKafkaRow.product_parent,
            product_title = eachKafkaRow.product_title,
            product_category = eachKafkaRow.product_category,
            star_rating = eachKafkaRow.star_rating, //int later?
            helpful_votes = eachKafkaRow.helpful_votes,
            total_votes = eachKafkaRow.total_votes,
            vine = eachKafkaRow.vine,
            verified_purchase = eachKafkaRow.verified_purchase,
            review_headline = eachKafkaRow.review_headline,
            review_body = eachKafkaRow.review_body,
            review_date = eachKafkaRow.review_date
          )
        }).toList.iterator
        connection.close()
        iter
      }
      )
      //write to the console
        /*  val query = customerData.writeStream
              .outputMode(OutputMode.Append())
              .format("console")
              .trigger(Trigger.ProcessingTime("5 seconds"))
              .start() */

      //write to HDFS
      val query = customerData.writeStream
        .outputMode(OutputMode.Append())
        .format("json")
        .option("path", "hdfs://manager.hourswith.expert:8020/user/mhart/reviews_streaming")
        .option("checkpointLocation", "hdfs://manager.hourswith.expert:8020/user/mhart/reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()
    }
    catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }
}


