import java.io.File
//import external library for csv
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader
import scala.util.Try
import scala.collection.mutable

//case class to represent a single hotel booking record
case class Booking(
  //only use the columns needed for analysis
  bookingID: String,
  originCountry: String,
  hotelName: String,
  bookingPrice: Double,
  discount: Double,
  profitMargin: Double,
  visitors: Int,
  destinationCountry: String,
  noOfDays : Int,
  rooms: Int
)

object HotelData {
  //map constants to CSV header names
  val BOOKING_ID = "Booking ID"
  val ORIGIN_COUNTRY = "Origin Country"
  val HOTEL_NAME = "Hotel Name"
  val BOOKING_PRICE = "Booking Price[SGD]"
  val DISCOUNT = "Discount"
  val PROFIT_MARGIN = "Profit Margin"
  val VISITORS = "No. Of People"
  val DESTINATION_COUNTRY = "Destination Country"
  val NO_OF_DAYS = "No of Days"
  val ROOMS = "Rooms"

  //read csv file and parse columns into Booking case class then returns a list of successfully parsed Booking objects
  def loadHotelDataset(filePath: String): List[Booking] = {
    var reader: Option[CSVReader] = None
    try {
      //open the csv file and read rows with headers
      reader = Some(CSVReader.open(new File(filePath)))
      val allRowsWithHeaders = reader.get.allWithHeaders()

      allRowsWithHeaders.flatMap { rawRow =>

        //sanitize column header names
        val row = rawRow.map { case (key, value) => (key.trim, value)}

        val result = for {
          price <- Try(row(BOOKING_PRICE).toDouble).toOption
          discount <- Try(row(DISCOUNT).toDouble).toOption
          profit <- Try(row(PROFIT_MARGIN).toDouble).toOption
          visitors <- Try(row(VISITORS).toInt).toOption
          noOfDays <- Try(row(NO_OF_DAYS).toInt).toOption
          rooms <- Try(row(ROOMS).toInt).toOption
        } yield Booking(
          bookingID = row(BOOKING_ID).trim,
          originCountry = row(ORIGIN_COUNTRY).trim,
          hotelName = row(HOTEL_NAME).trim,
          bookingPrice = price,
          discount = discount,
          profitMargin = profit,
          visitors = visitors,
          destinationCountry = row(DESTINATION_COUNTRY).trim,
          noOfDays = noOfDays,
          rooms = rooms
        )

        if (result.isEmpty) {
          //print error if row cannot be parsed
            println(s"Error parsing row.")
        }
        result
      }
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

object MainApp {
  def main(args: Array[String]): Unit = {

    //define filepath to dataset
    val filePath = "/Users/tim/Downloads/Hotel_Dataset.csv"
    val dataset: List[Booking] = HotelData.loadHotelDataset(filePath)

    //print error if dataset fails to load
    if (dataset.isEmpty) {
      println("Error. Failed to load dataset.")
      return
    }

    val q1 = dataset
      .groupBy(_.originCountry)
      .view.mapValues(_.size)
      .maxBy(_._2)

    println(s"${q1._1} has the highest number of bookings (${q1._2}) in the dataset.")
    
  }
}


