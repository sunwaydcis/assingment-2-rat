import java.io.File
//import external library for csv
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader
import scala.util.Try
import scala.collection.immutable
import scala.collection.mutable.StringBuilder

trait EconomicFactors {
  def criteria: String

  //return type is now list to handle ties
  def findBestOptions(data: List[Booking]): List[Booking]

  def formatValue(b: Booking): String

  //loops through the list of same economical options
  def printResult(data: List[Booking]): Unit = {
    val winners = findBestOptions(data)

    if (winners.isEmpty) {
      println(s"No data found for: $criteria")
    } else {
        winners.foreach { b =>
        println(s"${b.hotelName} (${formatValue(b)})")
      }
    }
  }
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
      reader.allWithHeaders().flatMap { rawRow =>
        parseRow(rawRow)
      }
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

//convert the list of Booking items into a String
trait StringConverter[T] {
  def convert(data: List[T]): String
}

//calculate the profit gained per person
abstract class ProfitPerPerson extends StringConverter[Booking] {
  // Formula: ((Profit Margin * Price) / 100) / NoOfPeople
  def calculateProfitPerPerson(b: Booking): Double = {
    if (b.visitors == 0) 0.0
    else ((b.profitMargin * b.bookingPrice) / 100.0) / b.visitors
  }
}

//function to find the average, min, and max booking price of a hotel room
class HotelCriteria extends StringConverter[Booking] {
  //helper case class to hold calculated results
  case class CriteriaResult(min: Double, max: Double, avg: Double, score: Double)

  //calculate criteria values for booking price, discount, and profit margin
  private def calculateCriteria(values: List[Double]): CriteriaResult = {
    val min = values.min
    val max = values.max
    val avg = values.sum / values.size
    val range = max - min

    //calculate economical score: 1 - ((average - min) / (max - min))
    //if Min equals Max, treat as 100% score
    val rawScore =
      if (range == 0.0)
        1.0
        else
          1.0 - ((avg - min) / range)

    CriteriaResult(min, max, avg, rawScore * 100.0)
  }

  override def convert(data: List[Booking]): String = {
      //group hotels by name, destination city, and country
      val grouped = data.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))

      //filter hotels that have >1 booking record
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

        //return a tuple with all details
        (name, city, country, priceCriteria, discountCriteria, profitCriteria, econScore)
      }

      val sortedStats = stats.toList.sortBy { case (name, city, country, _, _, _, _) =>
        (name, city, country)
      }

      //output most economical hotel
      val mostEconomical = stats.maxBy(_._7)
      val (name, city, country, _, _, _, score) = mostEconomical
      f"\nThe most economical hotel based on all 3 criteria is $name ($city, $country) with an overall economical score of $score%.2f%%"

/*
    //use stringbuilder to format output
      val sb = new StringBuilder

      // --- TABLE 1: PRICE STATISTICS ---
      sb.append("\n=== PART A: PRICE STATISTICS (Per Room) ===\n")
      sb.append(f"${"Hotel Name"}%-25s | ${"Location"}%-30s | ${"Min"}%-10s | ${"Max"}%-10s | ${"Avg"}%-10s | ${"Score"}%-8s\n")
      sb.append("-" * 110 + "\n")

      sortedStats.foreach { case (name, city, country, p, _, _, _) =>
        val loc = s"$city, $country"
        sb.append(f"$name%-25s | $loc%-30s | SGD${p.min}%6.2f | SGD${p.max}%6.2f | SGD${p.avg}%6.2f | ${p.score}%5.1f%%\n")
      }

      sb.toString()*/

  }
}


      class MostProfitableHotel extends ProfitPerPerson {
  override def convert(data: List[Booking]): String = {
    if (data.isEmpty)
      return "No data."

    val grouped = data.groupBy(_.hotelName)
    // Calculate the total profit for each hotel
    val stats = grouped.map { case (hotel, bookings) =>
      val totalProfit = bookings.map(calculateProfitPerPerson).sum
      (hotel, totalProfit)
    }
    // Find the hotel with the highest profit value
    val best = stats.maxBy(_._2)

    f"The most profitable hotel is ${best._1} with a profit of SGD${best._2}%.2f per person"
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
    /*val q2: List[EconomicFactors] = List(
      new BestBalancedStrategy()
    )

    println("\nQuestion 2 - Most Economical Hotel (Price/Discount/Profit Margin)")
    q2.foreach { strategy =>
      strategy.printResult(dataset)
    }*/


    //to print sorted hotels (avg/min/max price/economical score)
    val criteria: StringConverter[Booking] = new HotelCriteria()
    println(criteria.convert(dataset))

    /*
    //line to print/check the number of hotels
    //add map variables to check destination city/country
    val numberOfHotels = dataset
      .map(b => (b.hotelName))
      .distinct
      .size
    println(s"Total number of unique hotels: $numberOfHotels\n")*/

    //TO CHANGE - LOGIC TO GROUP BY HOTEL NAME, DESTINATION CITY, AND COUNTRY
    /*//group by both destination city and country to differentiate locations
      .groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))
      .view.mapValues(_.size)
      .maxBy(_._2)
    */

    //question 3
    println("\nQuestion 3 - The Most Profitable Hotel Per Person")
    val hotelProfit: StringConverter[Booking] = new MostProfitableHotel()
    println(hotelProfit.convert(dataset))
    
    }
  }
