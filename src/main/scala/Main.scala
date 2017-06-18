import java.util.concurrent.TimeUnit

import actors.Protocol._
import actors.{AggregatorActor, HaraldConsumerActor, ProducerActor}
import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import controllers.TestController
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.akka.{ActorReceiver, AkkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration

/**
  * Created by markmo on 13/05/2017.
  */
object Main extends App with TestHarness {

  val APP_NAME = "harald-analytics"
  val HARALD_CONSUMER = "harald-consumer"
  val WVA_CONSUMER = "wva-consumer"

  val config: Config = ConfigFactory.load()

  val host = config.getString("akka.host")
  val port = config.getString("akka.port")
  val sparkDriverHost = config.getString("spark.driver.host")
  val sparkDriverPort = config.getString("spark.driver.port")
  val httpInterface = config.getString("http.interface")
  val httpPort = config.getInt("http.port")
  val systemMessagesPath = config.getString("system-messages-path")

  val akkaConf = ConfigFactory.parseString(
    s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
        |akka.remote.netty.tcp.hostname = "$host"
        |akka.remote.netty.tcp.port = $port
        |""".stripMargin)

  implicit val system: ActorSystem = ActorSystem(APP_NAME, akkaConf)
  implicit val fm = ActorMaterializer()

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(APP_NAME)
    .set("spark.logConf", "true")
    .set("spark.driver.host", sparkDriverHost)
    .set("spark.driver.port", sparkDriverPort)

  val spark = SparkSession
    .builder()
    .appName(APP_NAME)
    .config(sparkConf)
    .getOrCreate()

  val aggregatorActor = system.actorOf(AggregatorActor.props(config, spark), "aggregator")

  // second arg is batch size
  val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

  val haraldConsumer = system.actorOf(HaraldConsumerActor.props(config), HARALD_CONSUMER)
  val wvaConsumer = system.actorOf(HaraldConsumerActor.props(config), WVA_CONSUMER)

  val wvaSystemMessageStream = AkkaUtils.createStream[WvaMessageResponsePayload](
    ssc,
    Props(classOf[MessageReceiver],
      s"akka.tcp://$APP_NAME@$host:$port/user/$WVA_CONSUMER"),
    "wvaSystemMessageReceiver"
  )

  //  val input: ReceiverInputDStream[UserMessage] = ssc.receiverStream[UserMessage](CustomReceiverInputDStream(StorageLevel.NONE))
  //  input.print()


  wvaSystemMessageStream.foreachRDD { rdd =>
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val messages = rdd map {
      case WvaMessageResponsePayload(WvaMessageResponse(botId, dialogId, message), timestamp) =>
        val ctx = message.context
        SystemMessage(
          botId,
          dialogId,
          ctx.conversationId,
          ctx.system.dialogStack.head.dialogNode,
          ctx.locationType.getOrElse(""),
          ctx.userLocation.getOrElse(""),
          ctx.userId.getOrElse(""),
          ctx.lpChatId.getOrElse(""),
          ctx.browser.getOrElse(""),
          ctx.device.getOrElse(""),
          ctx.operatingSystem.getOrElse(""),
          ctx.channel.getOrElse(""),
          ctx.resolutionStatus.getOrElse(""),
          ctx.sentiment.getOrElse(""),
          message.text.mkString(" "),
          message.intents.fold(List.empty[Intent])(_.map(i => Intent(i.intent, i.confidence))),
          message.entities.fold(List.empty[Entity])(_.map(e => Entity(e.entity, e.value))),
          timestamp
        )
    }
    val df = sqlContext.createDataFrame(messages)
    // write one file per batch
    df.coalesce(1).write.mode(SaveMode.Append).save(systemMessagesPath)
  }


  ssc.start()

  val producer = system.actorOf(ProducerActor.props(config), "producer")

  val testController = new TestController(producer)

  val bindingFuture = Http().bindAndHandle(testController.routes, httpInterface, httpPort)


  // generate test messages

  import system.dispatcher

  val interval = Duration.create(5, TimeUnit.SECONDS)

  system.scheduler.schedule(interval, interval) {
    producer ! makeWvaMessageResponse()
  }

  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)

}

class MessageReceiver(publisherURL: String) extends ActorReceiver {

  lazy private val remotePublisher = context.actorSelection(publisherURL)

  override def preStart(): Unit = {
    println("MessageReceiver starting up")
    remotePublisher ! SubscribeReceiver(context.self)
  }

  def receive = {
    case data => store(data)
  }

  override def postStop(): Unit = {
    println("MessageReceiver shutting down")
    remotePublisher ! UnsubscribeReceiver(context.self)
  }

}

object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

}