package actors

import java.util

import akka.actor.ActorRef
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by markmo on 13/05/2017.
  */
object Protocol {

  import JsEither._

  case class SubscribeReceiver(receiverActor: ActorRef)

  case class UnsubscribeReceiver(receiverActor: ActorRef)

  case class SystemMessage(botId: String,
                           dialogId: String,
                           conversationId: String,
                           dialogNode: String,
                           locationType: String,
                           userLocation: String,
                           userId: String,
                           lpChatId: String,
                           browser: String,
                           device: String,
                           operatingSystem: String,
                           channel: String,
                           resolutionStatus: String,
                           sentiment: String,
                           text: String,
                           intents: List[Intent],
                           entities: List[Entity],
                           timestamp: Long)

  case class Intent(name: String, confidence: Double = 0.0) {
    def toBean = new IntentBean(name, confidence)
  }

  case class Entity(name: String, value: String) {
    def toBean = new EntityBean(name, value)
  }

  case class User(id: String,
                  firstName: String,
                  lastName: String,
                  picture: String,
                  locale: String,
                  timezone: Int,
                  gender: String) {
    def toBean = new UserBean(id, firstName, lastName, picture, locale, timezone, gender)
  }

  case class Utterance(messageType: String,
                       sender: String,
                       text: String,
                       intent: Intent,
                       entities: List[Entity],
                       timestamp: Long,
                       time: String) {
    def toBean = new UtteranceBean(
      messageType,
      sender,
      text,
      intent.toBean,
      entities.map(_.toBean),
      timestamp,
      time
    )
  }

  case class UserMessage(user: User, utterance: Utterance) {
    def toBean = new UserMessageBean(user.toBean, utterance.toBean)
  }

  case class SystemTextMessage(messageType: String,
                               recipient: String,
                               text: String,
                               node: String,
                               nodeType: Option[String],
                               parent: Option[String],
                               flow: String,
                               path: List[String],
                               timestamp: Long,
                               time: String) {
    def toBean = new SystemTextMessageBean(messageType, recipient, text, node, nodeType.orNull, parent.orNull, flow, path.asJava, timestamp, time)
  }

  class IntentBean(@BeanProperty var name: String, @BeanProperty var confidence: Double = 0.0) {
    def toCase = Intent(name, confidence)
  }

  class EntityBean(@BeanProperty var name: String, @BeanProperty var value: String) {
    def toCase = Entity(name, value)
  }

  class UserBean(@BeanProperty var id: String,
                 @BeanProperty var firstName: String,
                 @BeanProperty var lastName: String,
                 @BeanProperty var picture: String,
                 @BeanProperty var locale: String,
                 @BeanProperty var timezone: Int,
                 @BeanProperty var gender: String) {
    def toCase = User(id, firstName, lastName, picture, locale, timezone, gender)
  }

  class UtteranceBean(@BeanProperty var messageType: String,
                      @BeanProperty var sender: String,
                      @BeanProperty var text: String,
                      @BeanProperty var intent: IntentBean,
                      @BeanProperty var entities: util.List[EntityBean],
                      @BeanProperty var timestamp: Long,
                      @BeanProperty var time: String) {
    def toCase = Utterance(
      messageType,
      sender,
      text,
      intent.toCase,
      entities.map(_.toCase).toList,
      timestamp,
      time
    )
  }

  class UserMessageBean(@BeanProperty var user: UserBean, @BeanProperty var utterance: UtteranceBean) {
    def toCase = UserMessage(user.toCase, utterance.toCase)
  }

  class SystemTextMessageBean(@BeanProperty var messageType: String,
                              @BeanProperty var recipient: String,
                              @BeanProperty var text: String,
                              @BeanProperty var node: String,
                              @BeanProperty var nodeType: String,
                              @BeanProperty var parent: String,
                              @BeanProperty var flow: String,
                              @BeanProperty var path: java.util.List[String],
                              @BeanProperty var timestamp: Long,
                              @BeanProperty var time: String) {
    def toCase = SystemTextMessage(messageType, recipient, text, node, Option(nodeType), Option(parent), flow, path.asScala.toList, timestamp, time)
  }

  implicit val intentFormat: Format[Intent] = Json.format[Intent]
  implicit val entityFormat: Format[Entity] = Json.format[Entity]
  implicit val userFormat: Format[User] = Json.format[User]
  implicit val utteranceFormat: Format[Utterance] = Json.format[Utterance]
  implicit val userMessageFormat: Format[UserMessage] = Json.format[UserMessage]
  implicit val textMessageFormat: Format[SystemTextMessage] = Json.format[SystemTextMessage]


  ////////////////// WVA MESSAGE FORMATS //////////////////

  /**
    * Used to specify the name of the widget to be displayed.
    *
    * Standard layout types include:
    *
    * * cc-validator Credit card widget for Make Payment flow
    * * form Creates a generic form from the field names in the Store object
    * * show-locations Map widget along with store location data for Find Nearest Store flow
    * * choose-location-type UI returns what type of location is being returned - outputs: zipcode, latlong
    * * request-geolocation-zipcode Requests user to input the desired zipcode
    * * request-geolocation-latlong Requests user to share current browser or device location
    *
    * @param name Layout name.
    * @param index Optional. When a dialog node has multiple lines of text along with a layout,
    *              an optional index property can be used to denote at which position the layout
    *              should be rendered. For example, to display the layout after the first string
    *              of text (array item 0), specify "index" : "0".
    */
  case class WvaLayout(name: String, index: Option[String], disableInput: Option[Boolean])

  implicit val wvaLayoutFormat: Format[WvaLayout] = (
    (__ \ 'name).format[String] and
    (__ \ 'index).formatNullable[String] and
    (__ \ 'disable_input).formatNullable[Boolean]
    )(WvaLayout.apply, unlift(WvaLayout.unapply))

  /**
    *
    * @param oneOf Requires input to be one of a range of values
    * @param someOf Requires input to be any of a range of values (for example a multiple selection textbox,
    *               select all that apply)
    * @param range Requires input to be in a range of values
    */
  case class WvaInputValidation(oneOf: Option[List[String]],
                                someOf: Option[List[String]],
                                range: Option[List[BigDecimal]])

  implicit val wvaInputValidationFormat: Format[WvaInputValidation] = Json.format[WvaInputValidation]

  /**
    * Rules that are used to validate the input provided by a user.
    *
    * @param regex A regular expression that indicates what values are allowed
    * @param message A message to display to users if their input does not meet the regular expression requirements
    */
  case class WvaFormLayoutValidation(regex: String, message: String)

  implicit val wvaFormLayoutValidationFormat: Format[WvaFormLayoutValidation] = Json.format[WvaFormLayoutValidation]

  /**
    * The form layout is a flexible widget that can be used anywhere in the dialog when a user input is needed
    * that does not need to be sent back to the dialog.
    *
    * @param name Variable name that can be used by the application to reference it
    * @param label Display name for the field in the form
    * @param required Optional. Specifies whether it is mandatory for a user to fill out the field.
    *                 False by default.
    * @param validations Optional. Rules that are used to validate the input provided by a user.
    */
  case class WvaFormLayoutField(name: String,
                                label: String,
                                required: Option[Boolean],
                                validations: Option[List[WvaFormLayoutValidation]])

  implicit val wvaFormLayoutFieldFormat: Format[WvaFormLayoutField] = Json.format[WvaFormLayoutField]

  case class WvaArgs(variables: List[String])

  implicit val wvaArgsFormat: Format[WvaArgs] = Json.format[WvaArgs]

  case class WvaAction(name: String, args: Option[Either[WvaArgs, Map[String, String]]])

  implicit val wvaActionFormat: Format[WvaAction] = Json.format[WvaAction]

  case class WvaIntent(confidence: Double, intent: String)

  implicit val wvaIntentFormat: Format[WvaIntent] = Json.format[WvaIntent]

  case class WvaEntity(entity: String, location: List[String], value: String)

  implicit val wvaEntityFormat: Format[WvaEntity] = Json.format[WvaEntity]

  case class WvaLogData(entities: Option[List[WvaEntity]],
                        intents: Option[List[WvaIntent]],
                        defaultVoyagerWorkspace: Option[Boolean],
                        showIntentLink: Option[Boolean],
                        workspaceId: Option[String],
                        workspaceType: Option[String],
                        topIntent: Option[WvaIntent])

  implicit val wvaLogDataFormat: Format[WvaLogData] = (
    (__ \ 'entities).formatNullable[List[WvaEntity]] and
    (__ \ 'intents).formatNullable[List[WvaIntent]] and
    (__ \ 'default_voyager_workspace).formatNullable[Boolean] and
    (__ \ 'show_intent_link).formatNullable[Boolean] and
    (__ \ 'workspace_id).formatNullable[String] and
    (__ \ 'workspace_type).formatNullable[String] and
    (__ \ 'wva_top_intent).formatNullable[WvaIntent]
    )(WvaLogData.apply, unlift(WvaLogData.unapply))

  /**
    * Provides an output dialog from the bot.
    *
    * @param text String response from bot. May be an array.
    * @param layout Optional. Used to specify the name of the widget to be displayed.
    * @param inputvalidation Optional.
    * @param store Directs the widget to store these values on the client system for integration and transactions,
    *              without returning data to the Watson Virtual Agent Cloud infrastructure in any way. The dialog
    *              waits for a callback with SUCCESS or FAILURE.
    * @param action Using action, we can instruct the channel to execute methods that require access to private
    *               variables, systems of record, or those that the bot typically cannot handle (for example,
    *               completing a payment transaction, updating the billing address, or retrieving private
    *               information from a system of record).
    * @param variables
    */
  case class WvaOutput(text: Option[Either[List[String], String]],
                       layout: Option[WvaLayout],
                       inputvalidation: Option[WvaInputValidation],
                       logData: Option[WvaLogData],
                       logMessages: Option[List[String]],
                       nodePosition: Option[String],
                       nodesVisited: Option[List[String]],
                       store: Option[List[WvaFormLayoutField]],
                       action: Option[WvaAction],
                       variables: Option[Boolean])

  implicit val wvaOutputFormat: Format[WvaOutput] = (
    (__ \ 'text).formatNullable[Either[List[String], String]] and
    (__ \ 'layout).formatNullable[WvaLayout] and
    (__ \ 'inputvalidation).formatNullable[WvaInputValidation] and
    (__ \ 'log_data).formatNullable[WvaLogData] and
    (__ \ 'log_messages).formatNullable[List[String]] and
    (__ \ 'node_position).formatNullable[String] and
    (__ \ 'nodes_visited).formatNullable[List[String]] and
    (__ \ 'store).formatNullable[List[WvaFormLayoutField]] and
    (__ \ 'action).formatNullable[WvaAction] and
    (__ \ 'variables).formatNullable[Boolean]
    )(WvaOutput.apply, unlift(WvaOutput.unapply))

  /**
    * The request property of context is used to call a method on the bot back end and then call the channel,
    * passing along any additional information if needed. For example, when the dialog invokes a request to
    * retrieve the list of stores near a location, the bot executes this method and then passes along the data
    * for nearest stores for the channel to display.
    *
    * @param name Name of the method being called
    * @param args Optional. List of arguments mapped as name: value
    * @param result Optional. Result passed back from the bot once the request is executed
    */
  case class WvaRequestContext(name: String, args: Option[Map[String, String]], result: Option[String])

  /**
    * Variable values can be persisted in the context section of the response.
    *
    * @param output
    * @param context
    */
  case class WvaResponse(output: WvaOutput, context: Option[Map[String, JsValue]])

  case class WvaDialogNode(dialogNode: String)

  implicit val wvaDialogNodeFormat: Format[WvaDialogNode] =
    (__ \ 'dialog_node).format[String].inmap(dialogNode => WvaDialogNode(dialogNode), (node: WvaDialogNode) => node.dialogNode)

  case class WvaMessageSystemContext(branchExited: Option[Boolean],
                                     branchExitedReason: Option[String],
                                     dialogRequestCounter: Int,
                                     dialogStack: List[WvaDialogNode],
                                     dialogTurnCounter: Int,
                                     dialogInProgress: Option[Boolean],
                                     nodeOutputMap: Option[Map[String, List[String]]])

  implicit val wvaMessageSystemContextFormat: Format[WvaMessageSystemContext] = (
    (__ \ 'branch_exited).formatNullable[Boolean] and
    (__ \ 'branch_exited_reason).formatNullable[String] and
    (__ \ 'dialog_request_counter).format[Int] and
    (__ \ 'dialog_stack).format[List[WvaDialogNode]] and
    (__ \ 'dialog_turn_counter).format[Int] and
    (__ \ 'dialog_in_progress).formatNullable[Boolean] and
    (__ \ '_node_output_map).formatNullable[Map[String, List[String]]]
    )(WvaMessageSystemContext.apply, unlift(WvaMessageSystemContext.unapply))

  case class WvaRequestAction(name: String)

  implicit val wvaRequestActionFormat: Format[WvaRequestAction] = Json.format[WvaRequestAction]

  case class WvaMessageContext(conversationId: String,
                               system: WvaMessageSystemContext,
                               request: Option[WvaRequestAction],
                               locationType: Option[String],
                               userLocation: Option[String],
                               userId: Option[String],
                               lpChatId: Option[String],
                               browser: Option[String],
                               device: Option[String],
                               operatingSystem: Option[String],
                               channel: Option[String],
                               resolutionStatus: Option[String],
                               sentiment: Option[String])

  implicit val wvaMessageContextFormat: Format[WvaMessageContext] = (
    (__ \ 'conversation_id).format[String] and
    (__ \ 'system).format[WvaMessageSystemContext] and
    (__ \ 'request).formatNullable[WvaRequestAction] and
    (__ \ 'location_type).formatNullable[String] and
    (__ \ 'user_location).formatNullable[String] and
    (__ \ 'user_id).formatNullable[String] and
    (__ \ 'lp_chat_id).formatNullable[String] and
    (__ \ 'browser).formatNullable[String] and
    (__ \ 'device).formatNullable[String] and
    (__ \ 'operating_system).formatNullable[String] and
    (__ \ 'channel).formatNullable[String] and
    (__ \ 'resolution_status).formatNullable[String] and
    (__ \ 'sentiment).formatNullable[String]
    )(WvaMessageContext.apply, unlift(WvaMessageContext.unapply))

  case class WvaAddress(address: String, lat: Double, lng: Double, timezone: String)

  case class WvaPhone(number: String, phoneType: String)

  case class WvaOpeningTimes(isOpen: Boolean,
                             open: Option[String],
                             close: Option[String],
                             openMeridiem: Option[String],
                             closeMeridiem: Option[String])

  case class WvaStoreLocation(address: WvaAddress,
                              days: List[WvaOpeningTimes],
                              description: Option[String],
                              email: Option[String],
                              hasDays: String,
                              hasPhones: String,
                              label: String,
                              phones: Option[List[WvaPhone]])

  case class WvaMessage(context: WvaMessageContext,
                        data: Option[List[Map[String, JsValue]]],
                        action: Option[WvaAction],
                        entities: Option[List[WvaEntity]],
                        inputvalidation: Option[WvaInputValidation],
                        intents: Option[List[WvaIntent]],
                        layout: Option[WvaLayout],
                        logData: Option[WvaLogData],
                        nodePosition: Option[String],
                        nodesVisited: Option[List[String]],
                        output: Option[WvaOutput],
                        text: List[String])

  implicit val wvaMessageFormat: Format[WvaMessage] = (
    (__ \ 'context).format[WvaMessageContext] and
    (__ \ 'data).formatNullable[List[Map[String, JsValue]]] and
    (__ \ 'action).formatNullable[WvaAction] and
    (__ \ 'entities).formatNullable[List[WvaEntity]] and
    (__ \ 'inputvalidation).formatNullable[WvaInputValidation] and
    (__ \ 'intents).formatNullable[List[WvaIntent]] and
    (__ \ 'layout).formatNullable[WvaLayout] and
    (__ \ 'log_data).formatNullable[WvaLogData] and
    (__ \ 'node_position).formatNullable[String] and
    (__ \ 'nodes_visited).formatNullable[List[String]] and
    (__ \ 'output).formatNullable[WvaOutput] and
    (__ \ 'text).format[List[String]]
    )(WvaMessage.apply, unlift(WvaMessage.unapply))

  case class WvaMessageResponse(botId: String, dialogId: String, message: WvaMessage) {

    def layoutName = message.layout match {
      case Some(l) => l.name
      case None => "none"
    }
  }

  implicit val wvaMessageResponseFormat: Format[WvaMessageResponse] = (
    (__ \ 'bot_id).format[String] and
    (__ \ 'dialog_id).format[String] and
    (__ \ 'message).format[WvaMessage]
    )(WvaMessageResponse.apply, unlift(WvaMessageResponse.unapply))

  case class WvaStartChatResponse(botId: String, dialogId: String, message: WvaMessage)

  case class WvaMessageRequest(message: Option[String], userID: Option[String], context: Option[WvaMessageContext])

  implicit val wvaMessageRequestFormat: Format[WvaMessageRequest] = Json.format[WvaMessageRequest]

  case class WvaMessageRequestPayload(botId: String, dialogId: String, message: WvaMessageRequest, timestamp: Long)

  implicit val wvaMessageRequestPayloadFormat: Format[WvaMessageRequestPayload] = (
    (__ \ 'bot_id).format[String] and
    (__ \ 'dialog_id).format[String] and
    (__ \ 'message).format[WvaMessageRequest] and
    (__ \ 'timestamp).format[Long]
    )(WvaMessageRequestPayload.apply, unlift(WvaMessageRequestPayload.unapply))

  case class WvaMessageResponsePayload(response: WvaMessageResponse, timestamp: Long)

  implicit val wvaMessageResponsePayloadFormat: Format[WvaMessageResponsePayload] = Json.format[WvaMessageResponsePayload]

  case class WvaErrorResponse(httpCode: String, httpMessage: String, moreInformation: String)

  implicit object JsEither {

    implicit def eitherReads[A, B](implicit A: Reads[A], B: Reads[B]): Reads[Either[A, B]] =
      Reads[Either[A, B]] { json =>
        A.reads(json) match {
          case JsSuccess(value, path) => JsSuccess(Left(value), path)
          case JsError(e1) => B.reads(json) match {
            case JsSuccess(value, path) => JsSuccess(Right(value), path)
            case JsError(e2) => JsError(JsError.merge(e1, e2))
          }
        }
      }

    implicit def eitherWrites[A, B](implicit A: Writes[A], B: Writes[B]): Writes[Either[A, B]] =
      Writes[Either[A, B]] {
        case Left(a) => A.writes(a)
        case Right(b) => B.writes(b)
      }

    implicit def eitherFormat[A, B](implicit A: Format[A], B: Format[B]): Format[Either[A, B]] =
      Format(eitherReads, eitherWrites)
  }

}

class JsonDeserializer[A: Reads] extends Deserializer[A] {

  private val stringDeserializer = new StringDeserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean) =
    stringDeserializer.configure(configs, isKey)

  override def deserialize(topic: String, data: Array[Byte]) =
    Json.parse(stringDeserializer.deserialize(topic, data)).as[A]

  override def close() =
    stringDeserializer.close()

}

class JsonSerializer[A: Writes] extends Serializer[A] {

  private val stringSerializer = new StringSerializer

  override def configure(configs: util.Map[String, _], isKey: Boolean) =
    stringSerializer.configure(configs, isKey)

  override def serialize(topic: String, data: A) =
    stringSerializer.serialize(topic, Json.stringify(Json.toJson(data)))

  override def close() =
    stringSerializer.close()

}