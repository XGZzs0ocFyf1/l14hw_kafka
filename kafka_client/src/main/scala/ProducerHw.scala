import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

object ProducerHw extends App {

  private val path = "/home/nyansus/Documents/opolsky/kafka_scala_example/kafka_client/src/main/resources/bwc.csv"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)



  val data = Utils.csvReader(path).map(_.asJson.noSpaces)


  data.foreach { m =>
    println(s"sending $m")
    producer.send(new ProducerRecord("books", m, m))
  }

  producer.close()
  println("done")
}
