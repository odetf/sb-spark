import org.json4s.JObject
import scala.io.BufferedSource
import scala.io.Source.fromFile
import org.json4s._
import org.json4s.jackson.Serialization.write

import java.io.{BufferedWriter, File, FileWriter}

object Lab1 {
    def main(args: Array[String]): Unit = {

        val source: BufferedSource = fromFile("C:/Users/Olga/Desktop/Данные/u.data")
        val lines: Seq[Array[Int]] = source.getLines.toList.map(string => string.split("\t").map(_.toInt))
        source.close()

        implicit val formats = DefaultFormats

        val all_films_data = JObject(JField("hist_film", createJSONFromList(get_film_statistics_by_id(9, lines))),
                                        JField("hist_all", createJSONFromList(get_all_films_statistics(lines))))

        val file: File = new File("lab01.json")

        val writer: BufferedWriter = new BufferedWriter(new FileWriter(file))
        //пишем строку в файл
        writer.write(write(all_films_data))
        //освобождаем ресурсы writer
        writer.close()
    }

    def get_film_statistics_by_id(id: Int, data: Seq[Array[Int]]): Array[Int] = {
        val films_by_id: Seq[Array[Int]] = data.filter(x => x(1) == id).sortBy(x => x(2))
        val array_marks: Array[Int] = Array(0, 0, 0, 0, 0)
        for (i <- films_by_id) {
            array_marks(i(2) - 1) = array_marks(i(2) - 1) + 1
        }
        array_marks
    }

    def get_all_films_statistics(data: Seq[Array[Int]]): Array[Int] = {
        val array_marks: Array[Int] = Array(0, 0, 0, 0, 0)
        for (i <- data) {
            array_marks(i(2) - 1) = array_marks(i(2) - 1) + 1
        }
        array_marks
    }

    def createJSONFromList(array: Array[Int]): JArray = {
        val json_array = JArray(array.map(integer => JInt(integer)).toList)
        json_array
    }
}
