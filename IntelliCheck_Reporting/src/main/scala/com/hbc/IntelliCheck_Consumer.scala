package com.hbc

import java.util
import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.postfixOps



/**   to understand the application better.
 *
 *  @constructor create a new IntellichecConsumerClass to consume Intellicheck data from kafka and load to teradata table.
 *  @param servers : provide the bootstrap servers.
 *  @param topic : takes the kafka topics to subscribe.
 *  @param groupid : takes a name to create a consumer group to kick start consumption from topic partitions.
 *  @param serializer : provide a deserializer method(class) to decode the understank key value.
 *  @param deserializer : provide a deserializer method to decode values into a readable format.
 *  @param autoOffset : Provide a offset type to read records(define a pattern to read).
 *  @param schemaRegestry : Provid the url of schemaregestry .
 */
case class IntellicheckConsumerClass(val servers:String,
                                     val topic: String,
                                     val groupid:String,
                                     val serializer:String = classOf[StringDeserializer].getCanonicalName,
                                     val deserializer:String = classOf[KafkaAvroDeserializer].getCanonicalName,
                                     val autoOffset:String="earliest",
                                     val schemaRegestry:String) extends LoggerClass {

  /**
   * Provides a service as described.
   *
   * Function takes the areguments provided to class
   * and maps the values . Return a full set of property
   * to configure kafka consumer.
   */

  def getProp():Properties = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, serializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset)
    //props.put("specific.avro.reader", specificAvroReader)
    props.put("schema.registry.url", schemaRegestry)
    props.put("consumer-timeout-ms", "30000")
    //props.put("session.timeout.ms", "300000")
    props
  }

  /**
   * Provides a service as described.
   *
   * The function is responsible for the consumption of kafka records.
   * it initalizes the consumer glass , subscribe to the topics provides and pool the records from partitios.
   * This acts like a main method of the class IntellicheckConsumerClass, and also handels all the
   * requirements . It also filters the log file containing intellicheck records and load into Teradata table.
   */

  def consume(props: Properties, spark: SparkSession, jprops: Properties, env:String) = {
    info("Initializing consume class with consumer properties")
    val conSumer = new KafkaConsumer[String, GenericRecord](props)
    info(s"subscribing to required topic ${topic}")
    conSumer.subscribe(util.Collections.singletonList(topic))
    while (true) {
      val records: ConsumerRecords[String,GenericRecord] = conSumer.poll(100)
      for(record <- records.asScala){

        val data:GenericRecord = record.value()
        info(s"reading the record having offset no ${record.offset()}")
        val header  = data.get("header").toString
        val body:String = data.get("body_records").toString
        info("filtering the records only having intellicheck columns.")
        if(body.contains("hbc_scan_time")){
          val icolnames = jprops.getProperty(env + ".dcolumns").split(",").toList
          val hcolnames = jprops.getProperty(env + ".hcolumns").split(",").toList
          info("creating dataframe for header records")
          val hdf = hfilter(header,hcolnames, spark )
          info("creating dataframe for body records")
          val intdf = dfilter(body, icolnames, spark)
          info("joining header and body dataframe")
          val finaldf = hdf.join(intdf,hdf("joinc")===intdf("joinc"),"right")
          val tempview:String = jprops.getProperty(env + ".tempview")
          info("creating final dataframe to insert")
          finaldf.createOrReplaceTempView(viewName=tempview)
          val Rawquery = jprops.getProperty(env + ".query")
          val query = Rawquery + tempview
          val insertdf = spark.sql(query)
          info("successfully created final df")
          info("inserting records to teradata table")
          InsertTeradata(insertdf, jprops, env)
        }else{
          warn(s"intellicheck records not found for offser valiue ${record.offset()}")
        }
      }}
  }


  /**
   * Provides a service as described.
   *
   * This method takes the json formatted string and
   * convert it into dataframe.
   */

  def hfilter(jsonstring:String, columns:Seq[String], spark: SparkSession):sql.DataFrame = {


    import spark.implicits._
    val df = spark.read.json(Seq(jsonstring).toDS)
    val df1 = df.select(columns.head, columns.tail: _*)
    //if(body.contains("hbc_payment_svce_swipecode"))
    val dffinal = df1.na.drop()
    val df2 = dffinal.withColumn("joinc", lit(1))
    df2

  }

  def dfilter(jsonstring:String, columns:Seq[String], spark: SparkSession):sql.DataFrame = {
    import spark.implicits._
    val df = spark.read.json(Seq(jsonstring).toDS)
    val df1 = df.select(columns.head, columns.tail: _*)
    val df2=df1.na.drop()
    val swipecode = if(jsonstring.contains("hbc_payment_svce_swipecode")){
      df.select("tender.hbc_payment_svce_swipecode").na.drop().collectAsList().get(0).getString(0)
    }else{
      "NULL"
    }
    val df3=df2.withColumn("hbc_payment_svce_swipecode", lit(swipecode)).withColumn("joinc", lit(1))
    df3
  }


  def InsertTeradata(df:sql.DataFrame, jprops: Properties, env : String)={
    println("Insert data started")
    val prop = new java.util.Properties
    prop.setProperty("driver", jprops.getProperty(env + ".teradriver"))
    prop.setProperty("user", jprops.getProperty(env + ".terauser"))
    prop.setProperty("password", jprops.getProperty(env + ".terapassword"))
    val ip = jprops.getProperty(env + ".teraip")
    val database = jprops.getProperty(env + ".teradatabase")
    val url = "jdbc:teradata://" + ip + "/database=" + database + ", TMODE=TERA"
    val table = jprops.getProperty(env + ".teratable")
    df.write.mode("append").jdbc(url, table, prop)
  }
}



object IntelliCheck_Consumer extends LoggerClass {
  def main(args: Array[String]) = {

    BasicConfigurator.configure()
    info("starting the progranmme and setting up spark session")
    val spark: SparkSession = SparkSession.builder.appName("test").getOrCreate()
    if (args.length == 0){
      val exception = new NullPointerException
      error("Provide environment information to process", exception)
    }
    val env = args(0)
    var jobproperties: Properties = null
    val proppath = getClass.getResource("/job.properties")
    if (proppath != null) {
      val source = Source.fromURL(proppath)
      jobproperties = new Properties()
      jobproperties.load(source.bufferedReader())
    }

    val servers = jobproperties.getProperty(env + ".servers")
    val topics =  jobproperties.getProperty(env + ".topics")
    val groupid = jobproperties.getProperty(env + ".groupid")
    val schemaregestry = jobproperties.getProperty(env + ".schemaregestry")
    val obj = IntellicheckConsumerClass(servers, topics, groupid, schemaRegestry = schemaregestry)
    val prop = obj.getProp()
    info("starting Consumer class to consume records from kafka brokers")
    obj.consume(prop, spark, jprops = jobproperties, env = env)

  }

}
