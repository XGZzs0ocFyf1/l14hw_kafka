import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

object ConsumerHw extends App {
  private val TOPIC = "books"
  //  private val TOPIC = "__consumer_offsets"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer1")
  props.put("max.poll.records", "15")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  consumer.subscribe(List(TOPIC).asJavaCollection)

  val partitions: util.List[PartitionInfo] = consumer
    .partitionsFor(TOPIC)

  //get collection of topic and partition info for changing offset for all partitions in this topic

  val t = consumer.poll(Duration.ofSeconds(3))

  val tpInfo: util.List[TopicPartition] = partitions
    .stream()
    .map(partInfo => new TopicPartition(partInfo.topic(), partInfo.partition))
    .collect(Collectors.toList[TopicPartition])
  consumer.seekToEnd(tpInfo) //here the code above


  val z = consumer.endOffsets(tpInfo)

  partitions.forEach { p =>
    val tp = new TopicPartition(p.topic(), p.partition())
    val currentOffset = consumer.endOffsets(tpInfo).get(tp)

    //change offset for current partition to desired
    consumer.seek(tp, currentOffset - 5)

    println(s"consumer's output for topic's ${p.topic()} partition ${p.partition()}")
    consumer
      .poll(Duration.ofSeconds(3))
      .records(p.topic())
      .asScala
      .foreach { r => println(r.value()) }
  }
  consumer.close()

}
