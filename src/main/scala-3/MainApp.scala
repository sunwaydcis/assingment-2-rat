import java.io.File
//import external library for csv
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader
import scala.util.Try
import scala.collection.mutable

//case class to represent a single hotel booking record
case class Booking(
  //only use the columns needed for analysis
  originCountry: String,
  hotelName: String,
  bookingPrice: Double,
  discount: Double,
  profitMargin: Double,
  visitors: Int
)

object HotelData {
  //map constants to CSV header names
  val ORIGIN_COUNTRY = "Origin Country"
  val HOTEL_NAME = "Hotel Name"
  val BOOKING_PRICE = "Booking Price[SGD]"
  val DISCOUNT = "Discount"
  val PROFIT_MARGIN = "Profit Margin"
  val VISITORS = "No. Of People"

  //read csv file then returns a list of successfully parsed Booking objects
  def loadHotelDataset(filePath: String): List[Booking] = {
    var reader: Option[CSVReader] = None
    try {
      //open the csv file and read rows with headers
      reader = Some(CSVReader.open(new File(filePath)))
      val allRowsWithHeaders = reader.get.allWithHeaders()
    } catch {
      //return error message and empty list if fail
      case e: Exception =>
        println(s"Error. Could not process $filePath")
        List.empty[Booking]
    } finally {
      reader.foreach(_.close())
    }
  }
}

//test - read csv file and output rows
object MainApp {
  def main(args: Array[String]): Unit = {

    //define filepath to dataset
    val filePath = "/Users/tim/Downloads/Hotel_Dataset.csv"

    //use CSVReader to handle parsing of csv file
    val reader = CSVReader.open(new File(filePath))
    val allRows = reader.allWithHeaders()
    allRows.foreach(println)
    reader.close()
  }
}


