package actors

import actors.Protocol._
import akka.actor.Actor
import akka.event.LoggingAdapter
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe
import cakesolutions.kafka.akka.{ConsumerRecords, KafkaConsumerActor, KafkaProducerActor, ProducerRecords}
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.Random

/**
  * Created by markmo on 13/05/2017.
  */
trait KafkaConfig {

  def config: Config

  def log: LoggingAdapter

  def randomString(len: Int = 5) = Random.alphanumeric.take(len).mkString("")

}

trait UserMessageConsumer extends KafkaConfig {
  this: Actor =>

  val userMessageExtractor = ConsumerRecords.extractor[java.lang.String, UserMessage]

  val userMessageConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config, new StringDeserializer(), new JsonDeserializer[UserMessage], self),
    "UserMessageConsumerActor"
  )

  def subscribeUserMessages(topics: List[String]) =
    userMessageConsumerActor ! Subscribe.AutoPartition(topics)

}

trait SystemTextMessageConsumer extends KafkaConfig {
  this: Actor =>

  val systemTextMessageExtractor = ConsumerRecords.extractor[java.lang.String, SystemTextMessage]

  val systemTextMessageConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config, new StringDeserializer(), new JsonDeserializer[SystemTextMessage], self),
    "SystemTextMessageConsumerActor"
  )

  def subscribeSystemTextMessages(topics: List[String]) =
    systemTextMessageConsumerActor ! Subscribe.AutoPartition(topics)

}

trait WvaUserMessageConsumer extends KafkaConfig {
  this: Actor =>

  val wvaUserMessageExtractor = ConsumerRecords.extractor[java.lang.String, WvaMessageRequestPayload]

  val wvaUserMessageConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config, new StringDeserializer(), new JsonDeserializer[WvaMessageRequestPayload], self),
    "WvaUserMessageConsumerActor"
  )

  def subscribeWvaUserMessageMessages(topics: List[String]) =
    wvaUserMessageConsumerActor ! Subscribe.AutoPartition(topics)

}

trait WvaSystemResponseConsumer extends KafkaConfig {
  this: Actor =>

  val wvaSystemResponseExtractor = ConsumerRecords.extractor[java.lang.String, WvaMessageResponse]

  val wvaSystemResponseConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config, new StringDeserializer(), new JsonDeserializer[WvaMessageResponse], self),
    "WvaSystemResponseConsumerActor"
  )

  def subscribeWvaSystemResponseMessages(topics: List[String]) =
    wvaSystemResponseConsumerActor ! Subscribe.AutoPartition(topics)

}

trait WvaUserMessageProducer extends KafkaConfig {
  this: Actor =>

  val wvaUserMessageProducerConf = KafkaProducer.Conf(
    bootstrapServers = config.getString("bootstrap.servers"),
    keySerializer = new StringSerializer(),
    valueSerializer = new JsonSerializer[WvaMessageRequestPayload]
  )

  val wvaUserMessageProducerActor = context.actorOf(KafkaProducerActor.props(wvaUserMessageProducerConf))

  def submitUserMessage(topics: List[String], message: WvaMessageRequestPayload) = {
    log.info(s"placing $message on ${topics.mkString(",")}")
    topics.foreach { topic =>
      wvaUserMessageProducerActor ! ProducerRecords(List(KafkaProducerRecord(topic, randomString(3), message)))
    }
  }

}

trait WvaSystemResponseProducer extends KafkaConfig {
  this: Actor =>

  val wvaSystemResponseProducerConf = KafkaProducer.Conf(
    bootstrapServers = config.getString("bootstrap.servers"),
    keySerializer = new StringSerializer(),
    valueSerializer = new JsonSerializer[WvaMessageResponsePayload]
  )

  val wvaSystemResponseProducerActor = context.actorOf(KafkaProducerActor.props(wvaSystemResponseProducerConf))

  def submitSystemMessage(topics: List[String], message: WvaMessageResponsePayload) = {
    //log.debug(s"placing $message on ${topics.mkString(",")}")
    topics.foreach { topic =>
      wvaSystemResponseProducerActor ! ProducerRecords(List(KafkaProducerRecord(topic, randomString(3), message)))
    }
  }

}
