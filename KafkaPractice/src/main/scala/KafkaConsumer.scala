import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val conf = new Properties
  conf.put("bootstrap.servers","localhost:9092")
  conf.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer")
  conf.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  conf.put("group.id", "something")

  val kafkaConsumer = new KafkaConsumer[Int,String](conf)
  kafkaConsumer.subscribe(Collections.singletonList("test"))

  while (true){

    val record = kafkaConsumer.poll(100)
    for (data <- record.asScala){
      println(data.key() , data.value())
    }

  }

}
