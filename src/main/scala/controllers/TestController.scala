package controllers

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import actors.Protocol
import play.api.libs.json.Json

/**
  * Post payloads to Kafka for testing
  *
  * Created by markmo on 27/05/2017.
  */
class TestController(producer: ActorRef) {

  import Protocol._
  import StatusCodes._

  val routes =
    path("test" / "system") {
      post {
        entity(as[String]) { data =>
          val json = Json.parse(data)
          val message = json.as[WvaMessageResponse]
          producer ! message
          complete(OK)
        }
      }
    }

}
