package actors

import actors.Protocol.{SubscribeReceiver, UnsubscribeReceiver, UserMessage}
import akka.actor._
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import com.google.firebase.database.DatabaseReference.CompletionListener
import com.google.firebase.database.{DatabaseError, DatabaseReference}
import com.typesafe.config.Config

import scala.collection.mutable

/**
  * Created by markmo on 13/05/2017.
  */
class HaraldConsumerActor(val config: Config) extends Actor
  with ActorLogging with UserMessageConsumer with SystemTextMessageConsumer {

  val receivers = new mutable.LinkedHashSet[ActorRef]()

  val companyId = config.getString("company.id")

  val updateUI = config.getBoolean("updateui")

  override def preStart(): Unit = {
    super.preStart()
    subscribeUserMessages(List("userMessages"))
    subscribeSystemTextMessages(List("systemTextMessages"))
  }

  def receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      log.debug(s"subscribe request from ${receiverActor.toString}")
      receivers += receiverActor

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      log.debug(s"unsubscribe request from ${receiverActor.toString}")
      receivers -= receiverActor

    case userMessageExtractor(records) =>
      records.pairs.foreach {
        case (Some(id), msg@UserMessage(user, utterance)) =>
          log.info("received UserMessage")
          userMessageConsumerActor ! Confirm(records.offsets)
          if (updateUI) {
            val interactionsRef = Firebase.ref(s"data/companies/$companyId/analytics/interactions")
            val utteranceBean = utterance.toBean
            interactionsRef.push().setValue(utteranceBean, new CompletionListener {
              override def onComplete(databaseError: DatabaseError, databaseReference: DatabaseReference): Unit =
                if (databaseError != null) {
                  log.error(databaseError.toException, databaseError.getMessage)
                } else {
                  log.debug("saved utterance")
                }
            })
            val visitorsRef = Firebase.ref(s"data/companies/$companyId/analytics/visitors/${user.id}")
            val userBean = user.toBean
            visitorsRef.setValue(userBean, new CompletionListener {
              override def onComplete(databaseError: DatabaseError, databaseReference: DatabaseReference): Unit =
                if (databaseError != null) {
                  log.error(databaseError.toException, databaseError.getMessage)
                } else {
                  log.debug("saved visitor")
                }
            })
          }
        case _ =>
      }

    case systemTextMessageExtractor(records) =>
      records.pairs.foreach {
        case (Some(id), systemTextMessage) =>
          log.info("received SystemTextMessage")
          systemTextMessageConsumerActor ! Confirm(records.offsets)
          //log.info(Json.prettyPrint(Json.toJson(systemTextMessage)))
          if (updateUI) {
            val interactionsRef = Firebase.ref(s"data/companies/$companyId/analytics/interactions")
            interactionsRef.push().setValue(systemTextMessage.toBean, new CompletionListener {
              override def onComplete(databaseError: DatabaseError, databaseReference: DatabaseReference): Unit =
                if (databaseError != null) {
                  log.error(databaseError.toException, databaseError.getMessage)
                } else {
                  log.debug("saved system text message")
                }
            })
          }
        case _ =>
      }

  }

}

object HaraldConsumerActor {

  def props(config: Config) = Props(new HaraldConsumerActor(config))

}