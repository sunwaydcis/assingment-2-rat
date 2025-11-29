//import external library for csv
import java.io.File
import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVReader

//test - read csv file and output rows
object MainApp {
  def main(args: Array[String]): Unit = {

    val filePath = "/Users/tim/Downloads/Hotel_Dataset.csv"

    //use CSVReader to handle parsing of csv file
    val reader = CSVReader.open(new File(filePath))
    val allRows = reader.allWithHeaders()
    allRows.foreach(println)
    reader.close()
  }
}


