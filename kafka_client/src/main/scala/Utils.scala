
import scala.io.Source
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

object Utils {

  private val QUOTE_FILTER_MASK = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  /**
   * This method handle input line and split only by commas that NOT surrounded by quotes
   * i.e. "\"5,000 Awesome Facts (About Everything!) (National Geographic Kids)\",National Geographic Kids,4.8,7665,12,2019,Non Fiction"
   * * @param in  line from csv file separated by commas
   */
  private def spliter(in: String) =  in.split(QUOTE_FILTER_MASK)




  def csvReader(path: String): List[BookInfo] = {
    val bs = Source.fromFile(path)
    val in = bs.getLines().drop(1)
   val data = in.foldLeft(List.empty[BookInfo])( (acc, ie) => acc :+ BookInfo.create(spliter(ie).map(_.trim)))
    bs.close
    data
  }


  case class BookInfo(
    name: String,
    author: String,
    userRating: Double,
    reviews: Long,
    price: Double, //вообще Big Decimal но в учебном проекте не важно IMHO
    year: Int,
    genre: String
  )

  object BookInfo{
    def create(in: Array[String]): BookInfo = {

      if (in.length != 7) {
        throw new IllegalArgumentException("Не могу обработать входные данные")
      }
      new BookInfo(
        name = in(0),
          author = in(1),
          userRating = in(2).toDouble,
          reviews = in(3).toLong,
          price = in(4).toDouble,
          year = in(5).toInt,
          genre = in(6)
      )
    }
  }


  def main(args: Array[String]): Unit = {
    val data = csvReader("/home/nyansus/Documents/opolsky/kafka_scala_example/kafka_client/src/main/resources/bwc.csv")
    println(s"data = ${data.head}")
    val json = data(0).asJson.noSpaces
    println(json)
  }
}


