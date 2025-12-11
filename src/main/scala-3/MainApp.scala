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
  //helper case class to hold calculated results
  case class LocalStats(min: Double, max: Double, avg: Double)

  def calculateLocalStats(values: List[Double]): LocalStats = {
    if (values.isEmpty)
      return LocalStats(0.0, 0.0, 0.0)
    LocalStats(values.min, values.max, values.sum / values.size)
  }

  //calculate economical score
  def calculateCriteria(value: Double, globalMin: Double, globalMax: Double, lowIsBetter: Boolean): Double = {
    val range = globalMax - globalMin

    //if all averages are identical, return score of 100
    if (range == 0.0)
      return 100.0

    //range from 0.0 (globalMin) to 1.0 (globalMax)
    val normalized = (value - globalMin) / globalMax

    //if low price/profit is better, invert score. if high discount is better, use direct normalized score
    if (lowIsBetter) 100.0 - (normalized * 100.0)
      else normalized * 100.0
  }
}

//find the average of each criteria
class HotelCriteria extends HotelScoreCalculator {
  override def convert(data: List[Booking]): String = {
    if (data.isEmpty)
      return "No data."

    //group hotels
    val grouped = data.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))

    val hotelAverages = grouped.map { case (key @ (name, city, country), bookings) =>
      val avgPrice = bookings.map(b => b.bookingPrice / b.rooms).sum / bookings.size
      val avgDiscount = bookings.map(_.discount).sum / bookings.size
      val avgProfit = bookings.map(_.profitMargin).sum / bookings.size

      (key, avgPrice, avgDiscount, avgProfit)
    //convert tuple to list
    }.toList

    //find min max range from the set of averages for all 3 criteria
    val allPrices = hotelAverages.map(_._2)
    val allDiscounts = hotelAverages.map(_._3)
    val allProfits = hotelAverages.map(_._4)

    val priceMinMax = (allPrices.min, allPrices.max)
    val discountMinMax = (allDiscounts.min, allDiscounts.max)
    val profitMinMax = (allProfits.min, allProfits.max)

    val scoredHotels = hotelAverages.map { case ((name, city, country), avgPrice, avgDiscount, avgProfit) =>
      val priceScore = calculateCriteria(avgPrice, priceMinMax._1, priceMinMax._2, lowIsBetter = true)
      val discountScore = calculateCriteria(avgDiscount, discountMinMax._1, discountMinMax._2, lowIsBetter = false)
      val profitScore = calculateCriteria(avgProfit, profitMinMax._1, profitMinMax._2, lowIsBetter = true)

      //combine scores
      val econScore = (priceScore + discountScore + profitScore) / 3

      (name, city, country, avgPrice, avgDiscount, avgProfit, econScore)
    }

    //rank based on score
    val mostEconomical = scoredHotels.maxBy(_._7)
    val (name, city, country, avgPrice, avgDiscount, avgProfit, score) = mostEconomical

    f"""$name ($city, $country) - Score $score%.2f%% | Avg Price/Room SGD$avgPrice%.2f | Avg Discount $avgDiscount%.2f%% | Avg Profit Margin $avgProfit%.2f%%
       |""".stripMargin
  }
}

class MostProfitableHotel extends HotelScoreCalculator {
  override def convert(data: List[Booking]): String = {
    if (data.isEmpty)
      return "No data."

    //group record by name, city, country
    val grouped = data.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))

    val hotelAverages = grouped.map { case (key @ (name, city, country), bookings) =>
      val totalVisitors = bookings.map(_.visitors.toDouble).sum
      val avgProfit = bookings.map(_.profitMargin).sum / bookings.size

      (key, totalVisitors, avgProfit)
    }.toList

    if (hotelAverages.isEmpty)
      return "Not enough data."

    //find min max range
    val allVisitors = hotelAverages.map(_._2)
    val allProfits = hotelAverages.map(_._3)

    val visitorMinMax = (allVisitors.min, allVisitors.max)
    val profitMinMax = (allProfits.min, allProfits.max)

    //higher visitors/profit margin is better
    val scoredHotels = hotelAverages.map { case ((name, city, country), totalVisitors, avgProfit) =>
      val visitorScore = calculateCriteria(totalVisitors, visitorMinMax._1, visitorMinMax._2, lowIsBetter = false)
      val profitScore = calculateCriteria(avgProfit, profitMinMax._1, profitMinMax._2, lowIsBetter = false)

      //combined score
      val overallScore = (visitorScore + profitScore) / 2.0

      (name, city, country, totalVisitors, avgProfit, overallScore)
    }

    //find the hotel with the highest combined score
    val best = scoredHotels.maxBy(_._6)
    val (name, city, country, totalVisitors, avgProfit, overallScore) = best

    f"""|$name ($city, $country) - Score $overallScore%.2f%% | Total Visitors $totalVisitors%.0f | Avg Profit Margin $avgProfit%.2f%%
        |""".stripMargin
  }
}

//main execution
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

    //question 1
    val q1 = dataset
      .groupBy(_.destinationCountry)
      .view.mapValues(_.size)
      .maxBy(_._2)

    println("Question 1 - Highest Number of Bookings")
    println(s"${q1._1} has the highest number of bookings (${q1._2}) in the dataset.")

    //question 2
    println("\nQuestion 2 - Most Economical Hotel")
    val criteria: StringConverter[Booking] = new HotelCriteria()
    println(criteria.convert(dataset))

    //question 3
    println("\nQuestion 3 - Most Profitable Hotel")
    val hotelProfitCombined: StringConverter[Booking] = new MostProfitableHotel()
    println(hotelProfitCombined.convert(dataset))
  }
}