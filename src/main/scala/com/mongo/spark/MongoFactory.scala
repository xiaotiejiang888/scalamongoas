import com.mongodb.casbah.{MongoConnection, MongoURI}

object MongoFactory {
  
  private val SERVER     = "172.23.5.158"
  private val PORT       = 27017
  private val DATABASE   = "mpush"

  @throws(classOf[Exception])
  def getConnection: MongoConnection = return MongoConnection(MongoURI("mongodb://mpush:talkingdata@172.23.5.158/mpush"))
  @throws(classOf[Exception])
  def closeConnection(conn: MongoConnection) { conn.close }

}

