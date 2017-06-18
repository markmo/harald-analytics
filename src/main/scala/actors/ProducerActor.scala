package actors

import actors.Protocol.{WvaMessageRequestPayload, WvaMessageResponsePayload}
import akka.actor.{Actor, ActorLogging, Props}
import com.typesafe.config.Config

/**
  * Created by markmo on 27/05/2017.
  */
class ProducerActor(val config: Config) extends Actor
  with ActorLogging with WvaUserMessageProducer with WvaSystemResponseProducer {

  def receive = {

    case msg@WvaMessageResponsePayload(_, _) =>
      submitSystemMessage(List("wvaSystemMessages"), msg)

    case msg@WvaMessageRequestPayload(_, _, _, _) =>
      log.debug("ProducerActor called with WvaMessageRequestPayload")
      submitUserMessage(List("wvaUserMessages"), msg)

  }

}

object ProducerActor {

  def props(config: Config) = Props(new ProducerActor(config))

}