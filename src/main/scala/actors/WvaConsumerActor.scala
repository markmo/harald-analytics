package actors

import akka.actor._
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import com.typesafe.config.Config
import actors.Protocol.{SubscribeReceiver, UnsubscribeReceiver}
import play.api.libs.json.Json

import scala.collection.mutable

/**
  * Created by markmo on 13/05/2017.
  */
class WvaConsumerActor(val config: Config) extends Actor
  with ActorLogging with WvaUserMessageConsumer with WvaSystemResponseConsumer {

  val receivers = new mutable.LinkedHashSet[ActorRef]()

  val companyId = config.getString("company.id")

  override def preStart(): Unit = {
    super.preStart()
    subscribeWvaUserMessageMessages(List("wvaUserMessages"))
    subscribeWvaSystemResponseMessages(List("wvaSystemMessages"))
  }

  def receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      log.debug(s"subscribe request from ${receiverActor.toString}")
      receivers += receiverActor

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      log.debug(s"unsubscribe request from ${receiverActor.toString}")
      receivers -= receiverActor

    case wvaUserMessageExtractor(records) =>
      records.pairs.foreach {
        case (Some(id), requestPayload) =>
          wvaUserMessageConsumerActor ! Confirm(records.offsets)
          println(Json.prettyPrint(Json.toJson(requestPayload)))
        case _ =>
      }

    case wvaSystemResponseExtractor(records) =>
      records.pairs.foreach {
        case (Some(id), systemResponseMessage) =>
          log.info("HaraldConsumerActor called")
          wvaSystemResponseConsumerActor ! Confirm(records.offsets)
          log.info(Json.prettyPrint(Json.toJson(systemResponseMessage)))
          receivers.foreach(_ ! systemResponseMessage)
        case _ =>
      }
  }

}

object WvaConsumerActor {

  def props(config: Config) = Props(new WvaConsumerActor(config))

}