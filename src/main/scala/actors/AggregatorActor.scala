package actors

import java.util.Calendar

import actors.Protocol.SystemMessage
import akka.actor.{Actor, ActorLogging, Props}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeConstants}

import scala.collection.mutable

/**
  * Created by markmo on 30/05/2017.
  */
class AggregatorActor(config: Config, spark: SparkSession) extends Actor with ActorLogging {

  import AggregatorActor._
  import spark.implicits._

  val systemMessagesPath = config.getString("system-messages-path")

  def receive = {

    case Start =>
      val Aggregate(year, qtr, month, week, day, hour) = calculateAggregate()

      val previousTimeSteps = 30

      val metricsMap: Map[String, Metrics] = Map(
        "year" -> calculateMetrics("year", previousTimeSteps, year),
        "qtr" -> calculateMetrics("qtr", previousTimeSteps, qtr),
        "month" -> calculateMetrics("month", previousTimeSteps, month),
        "week" -> calculateMetrics("week", previousTimeSteps, week),
        "day" -> calculateMetrics("day", previousTimeSteps, day),
        "hour" -> calculateMetrics("hour", previousTimeSteps, hour)
      )


  }

  private def calculateMetrics(timePeriod: String,
                               previousTimeSteps: Int,
                               aggregate: Map[Long, TimeAggregate]): Metrics = {

    val items = aggregate.keys.toList.sorted.takeRight(previousTimeSteps)

    // metrics
    val conversationsPerUserAverages: List[Double] = items map { key =>
      val users = aggregate(key).users
      val k = users.size
      val sum = users.values.map(_.size).sum
      sum / k.toDouble
    }

    val conversationTotals: List[Int] = items map { key =>
      aggregate(key).conversations.size
    }

    val messageTotals: List[Int] = items map { key =>
      aggregate(key).totalMessages
    }

    val sessionLengthAverages: List[Double] = items map { key =>
      val users = aggregate(key).users
      val k = users.size
      val sum = users.values.map { userConversations =>
        val k = userConversations.size
        val sum = userConversations.values.map { userConversation =>
          val times = userConversation.sorted
          val start = times.head
          val end = times.reverse.head
          end - start
        }.sum
        sum / k.toDouble
      }.sum
      sum / k.toDouble
    }

    val stepsPerUserAverages: List[Double] = items map { key =>
      val conversations = aggregate(key).conversations
      val users = aggregate(key).users
      val k = users.size
      val sum = users.values.map { userConversations =>
        val k = userConversations.size
        val sum = userConversations.keys.map { key =>
          conversations(key)
        }.sum
        sum / k.toDouble
      }.sum
      sum / k.toDouble
    }

    val userTotals: List[Int] = items.map(key => aggregate(key).users.size)

    Metrics(timePeriod,
      conversationsPerUserAverages,
      conversationTotals,
      messageTotals,
      sessionLengthAverages,
      stepsPerUserAverages,
      userTotals
    )
  }

  private def calculateAggregate(): Aggregate = {
    val yearValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val qtrValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val monthValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val weekValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val dayValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val hourValues = mutable.HashMap[Long, TimeAggregate]().withDefaultValue(TimeAggregate())
    val calendar = Calendar.getInstance
    val systemMessages = spark.read.load(systemMessagesPath).as[SystemMessage]
    systemMessages.foreach { msg =>
      calendar.setTimeInMillis(msg.timestamp)
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH)
      val day = calendar.get(Calendar.DAY_OF_MONTH)
      val hour = calendar.get(Calendar.HOUR_OF_DAY)
      val yearKey = new DateTime(year, 0, 1).getMillis
      val qtrKey = startOfQuarter(year, month).getMillis
      val monthKey = new DateTime(year, month, 1).getMillis
      val weekKey = startOfWeek(year, month, day).getMillis
      val dayKey = new DateTime(year, month, day).getMillis
      val hourKey = new DateTime(year, month, day, hour).getMillis
      val conversationId = msg.conversationId
      val userId = msg.userId
      val dimKey = DimensionKey(msg.resolutionStatus, msg.browser, msg.device, msg.operatingSystem)
      val timestamp = msg.timestamp
      updateTimeAggregate(yearValues, yearKey, userId, conversationId, dimKey, timestamp)
      updateTimeAggregate(qtrValues, qtrKey, userId, conversationId, dimKey, timestamp)
      updateTimeAggregate(monthValues, monthKey, userId, conversationId, dimKey, timestamp)
      updateTimeAggregate(weekValues, weekKey, userId, conversationId, dimKey, timestamp)
      updateTimeAggregate(dayValues, dayKey, userId, conversationId, dimKey, timestamp)
      updateTimeAggregate(hourValues, hourKey, userId, conversationId, dimKey, timestamp)
    }
    Aggregate(
      yearValues.toMap,
      qtrValues.toMap,
      monthValues.toMap,
      weekValues.toMap,
      dayValues.toMap,
      hourValues.toMap
    )
  }

  // TODO
  // not a pure function
  private def updateTimeAggregate(values: mutable.Map[Long, TimeAggregate],
                                  key: Long,
                                  userId: String,
                                  conversationId: String,
                                  dimKey: DimensionKey,
                                  timestamp: Long): Unit = {
    val value = values(key)
    val conversations: Map[String, Int] = value.conversations
    val users: Map[String, Map[String, List[Long]]] = value.users
    val userConversations: Map[String, List[Long]] = users(userId)
    val userConversation: List[Long] = userConversations(conversationId)
    val dimKeyHash: Int = dimKey.hashCode()
    values(key) = TimeAggregate(
      values = value.values + (dimKeyHash -> (value.values(dimKeyHash) + 1)),
      totalMessages = value.totalMessages + 1,
      conversations = conversations + (conversationId -> (conversations(conversationId) + 1)),
      users = users + (userId -> (userConversations + (conversationId -> (timestamp :: userConversation))))
    )
  }

  private def startOfQuarter(year: Int, month: Int) =
    if (month < 3) {
      new DateTime(year, 0, 1)
    } else if (month < 6) {
      new DateTime(year, 3, 1)
    } else if (month < 9) {
      new DateTime(year, 6, 1)
    } else {
      new DateTime(year, 9, 1)
    }

  private def startOfWeek(year: Int, month: Int, day: Int) =
    new DateTime(year, month, day).withDayOfWeek(DateTimeConstants.MONDAY)

}

object AggregatorActor {

  def props(config: Config, spark: SparkSession) = Props(new AggregatorActor(config, spark))

  case object Start

  case class Metrics(timePeriod: String,
                     conversationsPerUserAverages: List[Double],
                     conversationTotals: List[Int],
                     messageTotals: List[Int],
                     sessionLengthAverages: List[Double],
                     stepsPerUserAverages: List[Double],
                     userTotals: List[Int])

  case class DimensionKey(resolutionStatus: String, browser: String, device: String, operatingSystem: String)

  case class TimeAggregate(values: Map[Int, Int] = Map().withDefaultValue(0),
                           totalMessages: Int = 0,
                           conversations: Map[String, Int] = Map().withDefaultValue(0),
                           users: Map[String, Map[String, List[Long]]] = Map().withDefaultValue(Map().withDefaultValue(Nil)))

  case class Aggregate(year: Map[Long, TimeAggregate],
                       qtr: Map[Long, TimeAggregate],
                       month: Map[Long, TimeAggregate],
                       week: Map[Long, TimeAggregate],
                       day: Map[Long, TimeAggregate],
                       hour: Map[Long, TimeAggregate])

}