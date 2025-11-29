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


