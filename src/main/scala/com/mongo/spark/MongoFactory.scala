import com.mongodb.casbah.{MongoCollection, MongoConnection, MongoURI}

object MongoFactory {
  
  private val SERVER     = "172.23.6.82"
  private val PORT       = 27017
  private val DATABASE   = "mpush"
  private val COLLECTION = "stocks"

  @throws(classOf[Exception])
  def getConnection: MongoConnection = return MongoConnection(MongoURI("mongodb://mpush:talkingdata@172.23.6.82/mpush"))
  @throws(classOf[Exception])
  def getCollection(conn: MongoConnection): MongoCollection = return conn(DATABASE)(COLLECTION)
  @throws(classOf[Exception])
  def closeConnection(conn: MongoConnection) { conn.close }

}

