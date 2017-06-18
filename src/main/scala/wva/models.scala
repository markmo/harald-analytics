package wva

import spray.json.{DefaultJsonProtocol, JsValue}

// NOT USED
// Using Protocol classes instead and play json formats
// Play json used by scala-kafka-client library

/**
  * Created by markmo on 18/12/2016.
  */

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

/**
  * Rules that are used to validate the input provided by a user.
  *
  * @param regex A regular expression that indicates what values are allowed
  * @param message A message to display to users if their input does not meet the regular expression requirements
  */
case class WvaFormLayoutValidation(regex: String, message: String)

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

case class WvaArgs(variables: List[String])

case class WvaAction(name: String, args: Option[Either[WvaArgs, Map[String, String]]])

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

case class WvaMessageSystemContext(branchExited: Option[Boolean],
                                   branchExitedReason: Option[String],
                                   dialogRequestCounter: Int,
                                   dialogStack: List[Either[String, WvaDialogNode]],
                                   dialogTurnCounter: Int,
                                   dialogInProgress: Option[Boolean],
                                   nodeOutputMap: Option[Map[String, List[String]]])

case class WvaRequestAction(name: String)

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

case class WvaIntent(confidence: Double, intent: String)

case class WvaEntity(entity: String, location: List[String], value: String)

case class WvaLogData(entities: Option[List[WvaEntity]], intents: Option[List[WvaIntent]], defaultVoyagerWorkspace: Option[Boolean], showIntentLink: Option[Boolean])

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

case class WvaMessageResponse(botId: String, dialogId: String, message: WvaMessage) {

  def layoutName = message.layout match {
    case Some(l) => l.name
    case None => "none"
  }
}

case class WvaStartChatResponse(botId: String, dialogId: String, message: WvaMessage)

case class WvaMessageRequest(message: Option[String], userID: Option[String], context: Option[WvaMessageContext])

case class WvaMessageRequestPayload(botId: String, dialogId: String, message: WvaMessageRequest)

case class WvaErrorResponse(httpCode: String, httpMessage: String, moreInformation: String)

trait WvaJsonSupport extends DefaultJsonProtocol {
  implicit val wvaLayoutJsonFormat = jsonFormat(WvaLayout, "name", "index", "disable_input")
  implicit val wvaInputValidationJsonFormat = jsonFormat3(WvaInputValidation)
  implicit val wvaFormLayoutValidationJsonFormat = jsonFormat2(WvaFormLayoutValidation)
  implicit val wvaFormLayoutFieldJsonFormat = jsonFormat4(WvaFormLayoutField)
  implicit val wvaArgsJsonFormat = jsonFormat1(WvaArgs)
  implicit val wvaActionJsonFormat = jsonFormat2(WvaAction)
  implicit val wvaEntityJsonFormat = jsonFormat3(WvaEntity)
  implicit val wvaIntentJsonFormat = jsonFormat2(WvaIntent)
  implicit val wvaLogDataJsonFormat = jsonFormat(WvaLogData, "entities", "intents", "default_voyager_workspace", "show_intent_link")
  implicit val wvaOutputJsonFormat = jsonFormat(WvaOutput, "text", "layout", "inputvalidation", "log_data", "log_messages", "node_position", "nodes_visited", "store", "action", "variables")
  implicit val wvaRequestContextJsonFormat = jsonFormat3(WvaRequestContext)
  implicit val wvaResponseJsonFormat = jsonFormat2(WvaResponse)
  implicit val wvaDialogNodeJsonFormat = jsonFormat(WvaDialogNode, "dialog_node")
  implicit val wvaMessageSystemContextJsonFormat = jsonFormat(WvaMessageSystemContext, "branch_exited", "branch_exited_reason", "dialog_request_counter", "dialog_stack", "dialog_turn_counter", "dialog_in_progress", "_node_output_map")
  implicit val wvaRequestActionJsonFormat = jsonFormat1(WvaRequestAction)
  implicit val wvaMessageContextJsonFormat = jsonFormat(WvaMessageContext, "conversation_id", "system", "request", "location_type", "user_location", "user_id", "lp_chat_id", "browser", "device", "operating_system", "channel", "resolution_status", "sentiment")
  implicit val wvaAddressJsonFormat = jsonFormat4(WvaAddress)
  implicit val wvaPhoneJsonFormat = jsonFormat(WvaPhone, "number", "type")
  implicit val wvaOpeningTimesJsonFormat = jsonFormat5(WvaOpeningTimes)
  implicit val wvaStoreLocationJsonFormat = jsonFormat8(WvaStoreLocation)
  implicit val wvaMessageJsonFormat = jsonFormat(WvaMessage, "context", "data", "action", "entities", "inputvalidation", "intents", "layout", "log_data", "node_position", "nodes_visited", "output", "text")
  implicit val wvaMessageResponseJsonFormat = jsonFormat(WvaMessageResponse, "bot_id", "dialog_id", "message")
  implicit val wvaStartChatResponseJsonFormat = jsonFormat(WvaStartChatResponse, "bot_id", "dialog_id", "message")
  implicit val wvaMessageRequestJsonFormat = jsonFormat3(WvaMessageRequest)
  implicit val wvaMessageRequestPayloadJsonFormat = jsonFormat(WvaMessageRequestPayload, "bot_id", "dialog_id", "message")
  implicit val wvaErrorResponseJsonFormat = jsonFormat3(WvaErrorResponse)
}