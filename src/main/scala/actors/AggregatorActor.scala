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
      val TimeAggregates(year, qtr, month, week, day, hour) = makeTimeAggregates()

      val previousTimeSteps = 30

      val timeMeasures: Map[String, Measures] = Map(
        makeMeasures("year", previousTimeSteps, year),
        makeMeasures("qtr", previousTimeSteps, qtr),
        makeMeasures("month", previousTimeSteps, month),
        makeMeasures("week", previousTimeSteps, week),
        makeMeasures("day", previousTimeSteps, day),
        makeMeasures("hour", previousTimeSteps, hour)
      )

  }

  def makeMeasures(timePeriod: String,
                   previousTimeSteps: Int,
                   aggregate: Map[Long, TimeAggregate]): (String, Measures) = {

    val items: List[Long] = aggregate.keys.toList.sorted.takeRight(previousTimeSteps)

    // Measures

    val conversationsPerUserAverages: List[Double] = items map { timeKey =>
      val users = aggregate(timeKey).users
      val k = users.size
      val sum = users.values.map(_.size).sum
      sum / k.toDouble
    }

    val conversationTotals: List[Int] = items map { timeKey =>
      aggregate(timeKey).conversations.size
    }

    val messageTotals: List[Int] = items map { timeKey =>
      aggregate(timeKey).totalMessages
    }

    val sessionLengthAverages: List[Double] = items map { timeKey =>
      val users = aggregate(timeKey).users
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

    val stepsPerUserAverages: List[Double] = items map { timeKey =>
      val conversations = aggregate(timeKey).conversations
      val users = aggregate(timeKey).users
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

    val userTotals: List[Int] = items.map(timeKey => aggregate(timeKey).users.size)

    (timePeriod, Measures(timePeriod, Map(
      makeDoubleMeasureAggregate("conversationsPerUserAverages", conversationsPerUserAverages),
      makeIntMeasureAggregate("conversationTotals", conversationTotals),
      makeIntMeasureAggregate("messageTotals", messageTotals),
      makeDoubleMeasureAggregate("sessionLengthAverages", sessionLengthAverages),
      makeDoubleMeasureAggregate("stepsPerUserAverages", stepsPerUserAverages),
      makeIntMeasureAggregate("userTotals", userTotals),
      makeResolutionAggregate("resolved", aggregate, items),
      makeResolutionAggregate("unresolved", aggregate, items),
      makeResolutionAggregate("abandoned", aggregate, items),
      makeResolutionAggregate("escalated", aggregate, items)
    )))
  }

  def makeIntMeasureAggregate(measureName: String, data: List[Int]): (String, MeasureAggregate) = {
    val (sparklineData, thisPeriod, lastPeriod, change) = calculateIntChange(data)
    (measureName, IntMeasureAggregate(
      measureName,
      data,
      sparklineData,
      thisPeriod,
      lastPeriod,
      change
    ))
  }

  def calculateIntChange(data: List[Int]): (List[Int], Int, Int, Double) = {
    val sparklineData: List[Int] = data.reverse.take(5)
    val thisPeriod: Int = data.head
    val lastPeriod: Int = data.slice(1, 1).head
    val change: Double = if (lastPeriod == 0) {
      1.0
    } else {
      (thisPeriod - lastPeriod) / lastPeriod.toDouble
    }
    (sparklineData, thisPeriod, lastPeriod, change)
  }

  def makeDoubleMeasureAggregate(measureName: String, data: List[Double]): (String, MeasureAggregate) = {
    val (sparklineData, thisPeriod, lastPeriod, change) = calculateDoubleChange(data)
    (measureName, DoubleMeasureAggregate(
      measureName,
      data,
      sparklineData,
      thisPeriod,
      lastPeriod,
      change
    ))
  }

  def calculateDoubleChange(data: List[Double]): (List[Double], Double, Double, Double) = {
    val sparklineData: List[Double] = data.reverse.take(5)
    val thisPeriod: Double = data.head
    val lastPeriod: Double = data.slice(1, 1).head
    val change: Double = if (lastPeriod == 0) {
      1.0
    } else {
      (thisPeriod - lastPeriod) / lastPeriod
    }
    (sparklineData, thisPeriod, lastPeriod, change)
  }

  def makeResolutionAggregate(resolutionStatus: String,
                              aggregate: Map[Long, TimeAggregate],
                              items: List[Long]): (String, MeasureAggregate) = {
    val filterKeys: Seq[Int] = combineFilterKeys(resolutionStatus)
    val data: List[Int] = extractDimTotals(aggregate, items, filterKeys)
    val (sparklineData, totalThisPeriod, totalLastPeriod, changeThisPeriod) = calculateIntChange(data)
    (resolutionStatus, IntMeasureAggregate(
      resolutionStatus,
      data,
      sparklineData,
      totalThisPeriod,
      totalLastPeriod,
      changeThisPeriod
    ))
  }

  def extractDimTotals(aggregate: Map[Long, TimeAggregate],
                       items: List[Long],
                       filterKeys: Seq[Int]): List[Int] =
    items map { timeKey =>
      aggregate(timeKey).values.keys
        .filter(filterKeys.contains)
        .map(aggregate(timeKey).values)
        .sum
    }

  def makeTimeAggregates(): TimeAggregates = {
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
    TimeAggregates(
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
  def updateTimeAggregate(values: mutable.Map[Long, TimeAggregate],
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

  def startOfQuarter(year: Int, month: Int) =
    if (month < 3) {
      new DateTime(year, 0, 1)
    } else if (month < 6) {
      new DateTime(year, 3, 1)
    } else if (month < 9) {
      new DateTime(year, 6, 1)
    } else {
      new DateTime(year, 9, 1)
    }

  def startOfWeek(year: Int, month: Int, day: Int) =
    new DateTime(year, month, day).withDayOfWeek(DateTimeConstants.MONDAY)

  def combineFilterKeys(resolutionStatus: String): Seq[Int] = {
    val undefined = ""
    cartesianProductOf(List(
      List(resolutionStatus),
      undefined :: browsers,
      undefined :: devices,
      undefined :: operatingSystems
    )) map {
      case Seq(r, b, d, o) => DimensionKey(r, b, d, o).hashCode()
    }
  }

  def cartesianProductOf[T](xs: Traversable[Traversable[T]]): Seq[Seq[T]] =
    xs.foldLeft(Seq(Seq.empty[T])) { (x, y) =>
      for {
        a <- x.view
        b <- y
      } yield a :+ b
    }

}

object AggregatorActor {

  def props(config: Config, spark: SparkSession) = Props(new AggregatorActor(config, spark))

  case object Start

  case class Measures(timePeriod: String, aggregates: Map[String, MeasureAggregate])

  sealed trait MeasureAggregate

  case class IntMeasureAggregate(measureName: String,
                                 data: List[Int],
                                 sparklineData: List[Int],
                                 thisPeriod: Int,
                                 lastPeriod: Int,
                                 change: Double) extends MeasureAggregate

  case class DoubleMeasureAggregate(measureName: String,
                                    data: List[Double],
                                    sparklineData: List[Double],
                                    thisPeriod: Double,
                                    lastPeriod: Double,
                                    change: Double) extends MeasureAggregate

  case class TimeAggregates(year: Map[Long, TimeAggregate],
                            qtr: Map[Long, TimeAggregate],
                            month: Map[Long, TimeAggregate],
                            week: Map[Long, TimeAggregate],
                            day: Map[Long, TimeAggregate],
                            hour: Map[Long, TimeAggregate])

  case class TimeAggregate(values: Map[Int, Int] = Map().withDefaultValue(0),
                           totalMessages: Int = 0,
                           conversations: Map[String, Int] = Map().withDefaultValue(0),
                           users: Map[String, Map[String, List[Long]]] = Map().withDefaultValue(Map().withDefaultValue(Nil)))

  case class DimensionKey(resolutionStatus: String, browser: String, device: String, operatingSystem: String)

  val browsers = List("chrome", "firefox", "ie", "opera")
  val devices = List("apple", "android")
  val operatingSystems = List("linux", "windows", "macos")

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }

}