import java.io.File
//import external library for csv
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader
import scala.util.Try
import scala.collection.immutable

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
  noOfDays : Int,
  rooms: Int
)

class BestBalancedStrategy extends EconomicFactors {
  override def criteria: String = "Most Economical Hotel (Price/Discount/Profit Margin)"

  override def findBestOptions(data: List[Booking]): List[Booking] = {
    if (data.isEmpty)
      return List.empty

    //rank hotels based on: lowest price/profit, highest discount
    //pre-calculate ranks to avoid sorting the entire list 3 times
    val priceRanks = data.sortBy(b => b.bookingPrice / b.rooms).map(_.bookingID).zipWithIndex.toMap
    val discountRanks = data.sortBy(_.discount).reverse.map(_.bookingID).zipWithIndex.toMap
    val marginRanks = data.sortBy(_.profitMargin).map(_.bookingID).zipWithIndex.toMap

    //calculate a score for every hotel
    val scoredHotels = data.map { b =>
      val totalScore = priceRanks(b.bookingID) + discountRanks(b.bookingID) + marginRanks(b.bookingID)
      (b, totalScore)
    }

    //find what the best (lowest) score is
    val bestScore = scoredHotels.map(_._2).min

    //look at every item in the list, transforms the result (b) and adds it to the final list in a single pass
    scoredHotels.collect{case (b, totalScore) if totalScore == bestScore => b}
  }

  //format value of price per room
  override def formatValue(b: Booking): String = {
    val pricePerRoom = b.bookingPrice / b.rooms
    f"SGD${pricePerRoom}%.2f | ${b.discount}%% Off | ${b.profitMargin}%% Profit Margin"
  }
}

class BestPriceStrategy extends EconomicFactors {
  override def criteria: String = "Lowest Booking Price (Per Room)"

  override def findBestOptions(data: List[Booking]): List[Booking] = {
    if (data.isEmpty) return List.empty
    //find min value
    val minVal = data.map(b => b.bookingPrice / b.rooms).min
    //return all min value
    data.filter(b => (b.bookingPrice / b.rooms) == minVal)
  }

  override def formatValue(b: Booking): String = f"$$${b.bookingPrice / b.rooms}%.2f / room"
}

class BestDiscountStrategy extends EconomicFactors {
  override def criteria: String = "Highest Discount"

  override def findBestOptions(data: List[Booking]): List[Booking] = {
    if (data.isEmpty)
      return List.empty
    val maxVal = data.map(_.discount).max
    data.filter(_.discount == maxVal)
  }

  override def formatValue(b: Booking): String = s"${b.discount}%"
}

class BestMarginStrategy extends EconomicFactors {
  override def criteria: String = "Lowest Profit Margin"

  override def findBestOptions(data: List[Booking]): List[Booking] = {
    if (data.isEmpty)
      return List.empty
    val minVal = data.map(_.profitMargin).min
    data.filter(_.profitMargin == minVal)
  }

  override def formatValue(b: Booking): String = s"${b.profitMargin}%"
}

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
    val q2: List[EconomicFactors] = List(
      new BestBalancedStrategy()
    )

    println("\nQuestion 2 - Most Economical Hotel (Price/Discount/Profit Margin)")
    q2.foreach { strategy =>
      strategy.printResult(dataset)
    }

    //question 3
    println("\nQuestion 3 - The Most Profitable Hotel Per Person")
    val hotelProfit: StringConverter[Booking] = new MostProfitableHotel()
    println(hotelProfit.convert(dataset))
    
    }
  }
