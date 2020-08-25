import java.util.Properties
import java.io.File
import com.maxmind.geoip2.DatabaseReader
import java.net.InetAddress

import scala.io.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
 * Created by Aniesh on 23/10/20.
 * we will extract ip address from each message and then get Country ISO code. If it is US, we will send messages to partition 0 and for other countries,
 * we will send to the rest of the partitions using hash mod logic with partitions as 3 (which means data will for other Countries go into partition 1, 2, and 3).
 * Also if there are any invalid ips, we will send it to a different topic called retail_multi_invalid.
 * We will be using Java-based geoip2 provided by maxmind along with database with ip and country mapping.
 */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val en_properties = conf.getConfig(args(0))
    val properties = new Properties()

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      en_properties.getString("bootstrap.server"))
    properties.put(ProducerConfig.CLIENT_ID_CONFIG,
      "Produce log messages from file")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    val inputDir = args(1)
    val topicName = args(2)

    val logMessages = Source.fromFile(inputDir).getLines.toList

    val database = new File("src/main/resources/db/maxmind/GeoLite2-Country.mmdb")
    val reader = new DatabaseReader.Builder(database).build

    logMessages.foreach(message => {
      try {
        val ipAddr = message.split(" ")(0)
        val countryIsoCode = reader.
          country(InetAddress.getByName(ipAddr)).
          getCountry.
          getIsoCode
        val partitionIndex = if (countryIsoCode == "US") 2
        else countryIsoCode.hashCode() % 2
        val record = new ProducerRecord[String, String](topicName, partitionIndex, ipAddr, message)
        producer.send(record)
      } catch {
        case e: Exception => {
          val record = new ProducerRecord[String, String](topicName + "_invalid", message)
          producer.send(record)
        }
      }
    })
    producer.close()
  }
}