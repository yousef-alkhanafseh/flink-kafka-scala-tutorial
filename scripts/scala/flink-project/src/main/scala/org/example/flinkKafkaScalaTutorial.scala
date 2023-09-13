package org.apache.flink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows 
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import java.util.Properties

object flinkKafkaScalaTutorial {

  def main(args: Array[String]): Unit = {


	// define kafka topics
	val netflow_topic = "netflow"
	val cdr_topic = "cdr"
	val result_topic = "result"

	// prepare apache flink enviroment
	val env = StreamExecutionEnvironment.getExecutionEnvironment
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

	// prepare kafka consumers properties
	val properties = new Properties()
	properties.setProperty("bootstrap.servers", "localhost:9092")
	properties.setProperty("group.id", "flink-consumer-group")
	properties.setProperty("auto.offset.reset", "earliest")

	// prepare kafka cdr consumer
	val cdrConsumer = new FlinkKafkaConsumer[String](cdr_topic, new SimpleStringSchema(), properties)
	val cdrSource: DataStream[String] = env.addSource(cdrConsumer)

	// modify the schema of cdr stream
	val cdrStream = cdrSource
	    .map { jsonString =>
	    val fields = jsonString.split(",")
	    val CUSTOMER_ID = fields(0)
	    val PRIVATE_IP = fields(1)
	    val START_REAL_PORT = fields(2)
	    val END_REAL_PORT = fields(3)
	    val START_DATETIME = fields(4)
	    val END_DATETIME = fields(5)
	    (CUSTOMER_ID, PRIVATE_IP, START_REAL_PORT, END_REAL_PORT, START_DATETIME, END_DATETIME)}

	// prepare kafka netflow consumer
	val netflowConsumer = new FlinkKafkaConsumer[String](netflow_topic, new SimpleStringSchema(), properties)
	val netflowSource: DataStream[String] = env.addSource(netflowConsumer)

	// modify the schema of netflow stream
	val netflowStream = netflowSource
	    .map { jsonString =>
	    val fields = jsonString.split(",")
	    val NETFLOW_DATETIME = fields(0)
	    val SOURCE_ADDRESS = fields(1)
	    val SOURCE_PORT = fields(2)
	    val IN_BYTES = fields(3)
	    (NETFLOW_DATETIME, SOURCE_ADDRESS, SOURCE_PORT, IN_BYTES)}

	// join netflow and cdr streams based on the 2nd fields (SOURCE_ADDRESS and PRIVATE_IP), respectively.
	val joinedDataSet = netflowStream
	  .join(cdrStream)
	  .where(netflow => netflow._2)
	  .equalTo(cdr => cdr._2)
	   .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
	   .apply { (netflow, cdr) =>
	     (netflow._1.toInt, netflow._3.toInt,  netflow._4.toInt, cdr._1, cdr._3.toInt, cdr._4.toInt, cdr._5.toInt, cdr._6.toInt) // (NETFLOW_DATETIME, SOURCE_PORT, IN_BYTES, CUSTOMER_ID, START_REAL_PORT, END_REAL_PORT, START_DATETIME, END_DATETIME)
	   }

	// take observations which achive the following crateria:
	// A. NETFLOW_DATETIME betwen START_DATETIME and END_DATETIME
	// B. SOURCE_PORT between START_REAL_PORT and END_REAL_PORT
	// at the same time, aggregate the observations based on CUSTOMER_ID column and sum their IN_BYTES column
	val resultStream = joinedDataSet.filter{
	    x =>
	        x._1 >= x._7 &&
	        x._1 <= x._8 &&
	        x._2 >= x._5 &&
	        x._2 <= x._6
	    }.keyBy(3).sum(2).map { line =>
	    (line._4, line._3.toString)
	  }

	// print the result
	resultStream.print()


	// prepare kafka producer properties
	val kafkaProducerProperties = new Properties()
	kafkaProducerProperties.setProperty("bootstrap.servers", "localhost:9092")

	// define kafka producer
	val kafkaProducer = new FlinkKafkaProducer[String](
	        result_topic,
	        new SimpleStringSchema(),
	        kafkaProducerProperties)

	// sink resulted data to kafka "result" topic
	resultStream.map(x => x._1 + "," + x._2).addSink(kafkaProducer)

	// execute this code with the title of "Real-Time Stream Processing Using Flink and Kafka with Scala and Zeppelin (Part 2): Case Study"
	env.execute("Real-Time Stream Processing Using Flink and Kafka with Scala and Zeppelin (Part 2): Case Study")
  }

}
