import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {


    val conf = new Properties()
    conf.put("bootstrap.servers","localhost:9092")
    conf.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer")
    conf.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[Int,String](conf)
    val topic = "test"
    for(x <- (1 until 100)){

      val record:ProducerRecord[Int,String] = new ProducerRecord[Int,String](topic,x,s"next element is ${x+1}")
      kafkaProducer.send(record)
      Thread.sleep(1000)

    }


  }

}
