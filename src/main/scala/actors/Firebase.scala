package actors

import java.io.InputStream

import com.google.firebase.auth.FirebaseCredentials
import com.google.firebase.database.{DatabaseReference, FirebaseDatabase}
import com.google.firebase.{FirebaseApp, FirebaseOptions}

/**
  * Created by markmo on 13/05/2017.
  */

case class FirebaseException(msg: String) extends Exception(msg)

object Firebase {

  private val serviceAccount: InputStream = getClass.getResourceAsStream("/bot-tools-firebase-adminsdk-4hd6m-d1821bd80f.json")

  private val options = new FirebaseOptions.Builder()
    .setCredential(FirebaseCredentials.fromCertificate(serviceAccount))
    .setDatabaseUrl("https://bot-tools.firebaseio.com")
    .build()

  FirebaseApp.initializeApp(options)

  private val database = FirebaseDatabase.getInstance()

  def ref(path: String): DatabaseReference = database.getReference(path)

}
