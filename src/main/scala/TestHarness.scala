import actors.Protocol._

import scala.collection.mutable
import scala.util.Random

/**
  * Created by markmo on 28/05/2017.
  */
trait TestHarness {

  val rand = Random

  val browsers = mutable.LinkedHashMap(
    "chrome" -> .25,
    "firefox" -> .25,
    "ie" -> .25,
    "opera" -> .25
  )

  val devices = mutable.LinkedHashMap(
    "apple" -> .5,
    "android" -> .5
  )

  val operatingSystems = mutable.LinkedHashMap(
    "linux" -> .33,
    "windows" -> .34,
    "macos" -> .33
  )

  val resolutionStatus = mutable.LinkedHashMap(
    "resolved" -> .5,
    "unresolved" -> .25,
    "abandoned" -> .17,
    "escalated" -> .08
  )

  def weightedProb[A](prob: mutable.LinkedHashMap[A, Double]): A = {
    def weighted(todo: Iterator[(A, Double)], rand: Double, accum: Double = 0): A = todo.next match {
      case (s, i) if rand < (accum + i) => s
      case (_, i) => weighted(todo, rand, accum + i)
    }
    weighted(prob.toIterator, Random.nextDouble)
  }

  def makeWvaMessageResponse() =
    WvaMessageResponsePayload(
      WvaMessageResponse(
        botId = "e7c7e1cf-0937-4036-ae00-78787845d00f",
        dialogId = "c39f9429-f6a9-4562-8577-b839060d4385",
        message = WvaMessage(
          context = WvaMessageContext(
            conversationId = "c790c8b7-eb39-41a5-919a-56f37efea09c",
            system = WvaMessageSystemContext(
              branchExited = None,
              branchExitedReason = None,
              dialogRequestCounter = 1,
              dialogStack = List(WvaDialogNode("node_11_1484782529406")),
              dialogTurnCounter = 1,
              dialogInProgress = None,
              nodeOutputMap = None
            ),
            request = None,
            locationType = None,
            userLocation = None,
            userId = None,
            lpChatId = None,
            browser = Some(weightedProb(browsers)),
            device = Some(weightedProb(devices)),
            operatingSystem = Some(weightedProb(operatingSystems)),
            channel = None,
            resolutionStatus = if (rand.nextFloat() > .75) Some(weightedProb(resolutionStatus)) else None,
            sentiment = None
          ),
          data = None,
          action = None,
          entities = None,
          inputvalidation = None,
          intents = None,
          layout = None,
          logData = None,
          nodePosition = None,
          nodesVisited = None,
          output = None,
          text = List("Hello")
        )
      ),
      System.currentTimeMillis
    )

}
