package org.apache.flink
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import java.util.Properties
import org.json4s.Formats
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.formats.json.JsonDeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import java.time.Duration
import java.util.Properties

import org.json4s._
import org.json4s.native.JsonMethods

object flinkKafkaScalaTutorial {

  case class Order(customer_id: String, location: String, order_date: String, order_id: String, price: String, product_id: String, seller_id: String, status: String)

  def main(args: Array[String]): Unit = {
          implicit val formats: Formats = DefaultFormats

          val env = StreamExecutionEnvironment.getExecutionEnvironment
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
          env.enableCheckpointing(100);
          env.getConfig.setAutoWatermarkInterval(5000L); // poll watermark every second

          // prepare kafka sources (producers)
          val properties = new Properties()
          properties.setProperty("bootstrap.servers", "localhost:9092")
          properties.setProperty("group.id", "flink-consumer-group")

          val topic1 = "orders"
          val kafkaConsumer1 = new FlinkKafkaConsumer[String](topic1, new SimpleStringSchema(), properties)

	  val kafkaConsumer11 = kafkaConsumer1.assignTimestampsAndWatermarks(
	     WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofMinutes(1)))


          val ordersSource: DataStream[String] = env.addSource(kafkaConsumer11)

          val ordersStream = ordersSource.flatMap(raw => parseOrder(raw))

	  ordersStream.print()
          ordersStream.print
          println(ordersStream)
          env.execute("Yousef Trendyol Data Engineer Case")
}
  def parseOrder(raw: String): Option[Order] = {
    implicit val formats: Formats = DefaultFormats
    try {
      val json = JsonMethods.parse(raw)
      Some(json.extract[Order])
    } catch {
      case _: Throwable => None
    }
  }

}
