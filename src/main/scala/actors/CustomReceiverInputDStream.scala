package actors

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Created by markmo on 14/05/2017.
  */
case class CustomReceiverInputDStream[T](override val storageLevel: StorageLevel) extends Receiver[T](storageLevel) {

  def onStart(): Unit = {
    println("CustomerReceiver.START")
  }

  def onStop(): Unit = {
    println("CustomerReceiver.STOP")
  }

}
