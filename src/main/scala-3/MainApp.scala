import java.io.File
//import external library for csv
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader
import scala.util.Try
import scala.collection.immutable
import scala.collection.mutable.StringBuilder

//convert the list of booking items into a String
trait StringConverter[T] {
  def convert(data: List[T]): String
}

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
  destinationCity: String,
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
  val DESTINATION_CITY = "Destination City"
  val NO_OF_DAYS = "No of Days"
  val ROOMS = "Rooms"

  //regex to remove non-numeric characters (except decimal points)
  private val Regex = """[^\d.]""".r

  //function to sanitize cell data, converts double to string and removes non-numeric characters
  def cleanDouble(s: String): Option[Double] = {
    //removes all non-digit/non-dot chars and parses
    Try(Regex.replaceAllIn(s, "").toDouble).toOption
  }

  //function to sanitize cell data, converts int to string and removes non-numeric characters
  def cleanInt(s: String): Option[Int] = {
    //remove all non-digit/non-dot chars and parses
    Try(Regex.replaceAllIn(s, "").toInt).toOption
  }

  //read csv file and parse columns into Booking case class then returns a list of successfully parsed Booking objects
  def loadHotelDataset(filePath: String): List[Booking] = {
    //open the csv file and read rows with headers. throw error if not found or cannot read
    val file = new File(filePath)
    if (!file.exists) {
      println(s"File not found: $filePath")
      return List.empty
    }
    val reader = CSVReader.open(file)

    try {
      val allBookings = reader.allWithHeaders().flatMap { rawRow =>
        parseRow(rawRow)
      }
      //logic to remove duplicates using bookingID
      allBookings
        .groupBy(_.bookingID) //group records by bookingID
        .map { case (_, rows) => rows.head } //keep only the first record for each bookingID
        .toList

    } catch {
      //return error message and empty list if fail
      case e: Exception =>
        println(s"Error. Could not process $filePath")
        println(s"Error processing file: ${e.getMessage}")
        List.empty[Booking]
    } finally {
      reader.close()
    }
  }

  //function to parse row
  private def parseRow(rawRow: Map[String, String]): Option[Booking] = {
    //sanitize column header names
    val row = rawRow.map { case (key, value) => (key.trim, value) }

    val result = for {
      price <- cleanDouble(row(BOOKING_PRICE))
      discount <- cleanDouble(row(DISCOUNT))
      profit <- cleanDouble(row(PROFIT_MARGIN))
      visitors <- cleanInt(row(VISITORS))
      noOfDays <- cleanInt(row(NO_OF_DAYS))
      rooms <- cleanInt(row(ROOMS))
    } yield Booking(
      bookingID = row(BOOKING_ID).trim,
      originCountry = row(ORIGIN_COUNTRY).trim,
      hotelName = row(HOTEL_NAME).trim,
      bookingPrice = price,
      discount = discount,
      profitMargin = profit,
      visitors = visitors,
      destinationCountry = row(DESTINATION_COUNTRY).trim,
      destinationCity = row(DESTINATION_CITY).trim,
      noOfDays = noOfDays,
      rooms = rooms
    )

    //print error if row cannot be parsed
    if (result.isEmpty) {
      println(s"Error parsing row.")
    }
    result
  }
}

//parent class with formula for Q2 & Q3
abstract class HotelScoreCalculator extends StringConverter[Booking] {
  // Helper case class to hold calculated results
  case class CriteriaResult(min: Double, max: Double, avg: Double, score: Double)

  def calculateCriteria(values: List[Double]): CriteriaResult = {
    val min = values.min
    val max = values.max
    val avg = values.sum / values.size
    val range = max - min

    //calculate economical score

    val rawScore =
      if (range == 0.0) 1.0
      else 1.0 - ((avg - min) / range)//if Min equals Max treat as 100% score

    CriteriaResult(min, max, avg, rawScore * 100.0)
  }
}

//find the average, min, and max booking price of a hotel room
class HotelCriteria extends HotelScoreCalculator {
  override def convert(data: List[Booking]): String = {
    //group hotels 
    val grouped = data.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))

    //filter hotels with more than 1 record
    val validHotels = grouped.filter { case (_, bookings) =>
      bookings.size > 1
    }

    val stats = validHotels.map { case ((name, city, country), bookings) =>
      val pricePerRoom = bookings.map(b => b.bookingPrice / b.rooms)
      val discount = bookings.map(b => b.discount)
      val profitMargin = bookings.map(b => b.profitMargin)

      //calculate each criteria and economical score
      val priceCriteria = calculateCriteria(pricePerRoom)
      val discountCriteria = calculateCriteria(discount)
      val profitCriteria = calculateCriteria(profitMargin)

      val econScore = (priceCriteria.score + discountCriteria.score + profitCriteria.score) / 3

      //return all details in a tuple
      (name, city, country, priceCriteria, discountCriteria, profitCriteria, econScore)
    }

    //output most economical hotel
    val mostEconomical = stats.maxBy(_._7)
    val (name, city, country, _, _, _, score) = mostEconomical

    f"\nThe most economical hotel based on all 3 criteria is $name ($city, $country) with an overall economical score of $score%.2f%%"
  }
}

class MostProfitableHotel extends HotelScoreCalculator {
  override def convert(data: List[Booking]): String = {
    if (data.isEmpty) return "No data."

    //group record by name, city, country
    val grouped = data.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))
    val validHotels = grouped.filter { case (_, bookings) => bookings.size > 1 }

    if (validHotels.isEmpty) return "Not enough data for Question 3."

    val stats = validHotels.map { case ((name, city, country), bookings) =>
      val visitorValues = bookings.map(_.visitors.toDouble)
      val profitValues = bookings.map(_.profitMargin)

      //inherit calculation logic
      val visitorStats = calculateCriteria(visitorValues)

      //average for profit
      val avgProfit = profitValues.sum / profitValues.size

      (name, city, country, visitorStats, avgProfit)
    }

    //find answer by average profit
    val best = stats.maxBy(_._5)
    val (name, city, country, vStats, avgProfit) = best

    f"""
       |The most profitable hotel is $name ($city, $country).
       |----------------------------------------------------
       |Average Profit Margin : $avgProfit%.2f%%
       |Visitor Consistency   : ${vStats.score}%.2f%% (Score based on formula)
       |Visitor Stats         : Min ${vStats.min.toInt}, Max ${vStats.max.toInt}, Avg ${vStats.avg}%.1f
       |""".stripMargin
  }
}


//main execution
object MainApp {
  def main(args: Array[String]): Unit = {
    //define filepath to dataset
    val filePath = "C:\\Users\\ILLEGEAR\\Downloads\\Hotel_Dataset.csv"
    val dataset: List[Booking] = HotelData.loadHotelDataset(filePath)

    //print error if dataset fails to load
    if (dataset.isEmpty) {
      println("Error. Failed to load dataset.")
      return
    }

    //question 1
    val q1 = dataset
      .groupBy(_.destinationCountry)
      .view.mapValues(_.size)
      .maxBy(_._2)

    println("Question 1 - Highest Number of Bookings")
    println(s"${q1._1} has the highest number of bookings (${q1._2}) in the dataset.")

    //question 2
    println("\nQuestion 2 - The Most Economical Hotel")
    val criteria: StringConverter[Booking] = new HotelCriteria()
    println(criteria.convert(dataset))

    //question 3
    println("\nQuestion 3 - The Most Profitable Hotel")
    val hotelProfit: StringConverter[Booking] = new MostProfitableHotel()
    println(hotelProfit.convert(dataset))
  }
}