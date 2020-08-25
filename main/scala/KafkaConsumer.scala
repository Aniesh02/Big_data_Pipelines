import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
 * Created by Aniesh on 23/10/20.
 * Auto commit is enabled to be true
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val en_properties = conf.getConfig(args(0))
    val properties = new Properties()

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      en_properties.getString("bootstrap.server"))
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG,
      "Consume messages from topic")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cgb3")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](properties)
    val partitions = Set(new TopicPartition(args(1), args(2).toInt))
    consumer.assign(partitions)
    consumer.seekToBeginning(consumer.assignment())

    while(true) {
      val records = consumer.poll(500)
      for(record <- records) {
        println("Received message: (" + record.key()
          + ", " + record.value()
          + ") from " + record.partition()
          + " at offset " + record.offset())
      }
      Thread.sleep(100)
    }
  }
}