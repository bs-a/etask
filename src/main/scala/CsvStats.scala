import java.nio.file.{FileSystem, FileSystems, Path}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}

import scala.concurrent.Future
import akka.util.ByteString

object CsvStats {

  implicit val system: ActorSystem = ActorSystem("GraphCycle")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  type SourceFiles = Source[Path, NotUsed]#Repr[Path]#Repr[ByteString]

  val fs: FileSystem = FileSystems.getDefault

  val directorySource: String => Source[Path, NotUsed] = (directory: String) => Directory
    .ls(fs.getPath(directory))

  val files = Flow[Path].flatMapConcat(path => FileIO.fromPath(path))

  val foldFlowInt = Flow[Map[String, Int]].fold[Map[String, List[Int]]](Map()) { (acc, trans) =>
    val sId = trans.keySet.toSeq.head
    val humidity = trans.values.toSeq.head
    if (acc.keySet.contains(sId)) {
      val humidityAcc: List[Int] = acc(sId)
      acc ++ Map(sId -> (humidityAcc :+ humidity))
    } else {
      acc ++ Map(sId -> List(humidity))
    }
  }

  case class LineStat(sensorId: String, min: Int, avg: Double, max: Int) {
    override def toString: String = {
      s"$sensorId,${if(min<0) "NaN" else min},${if(avg<0) "NaN" else avg},${if(max<0) "NaN" else max  }"
    }
  }

  object LineStat {
    implicit val avgSort: Ordering[LineStat] = Ordering.fromLessThan(_.avg > _.avg)
  }

  val flowMap = Flow[Map[String, String]].map { l =>
    val sensorId = l("sensor-id")
    val humidity = {
      val hval = l("humidity")
      if(hval == "NaN") -1
      else hval.toInt
    }
    Map(sensorId -> humidity)
  }

  val flowHeader = Flow[Map[String, String]].filter { l =>
    l.values.toSet != l.keys
  }

  val flowStats = Flow[Map[String, List[Int]]].map { l =>

    l.keySet.map { sensorId =>
      val humidity = l(sensorId)
      if(humidity.head == -1 && humidity.forall(_ == humidity.head)) {
        LineStat(sensorId, -1, -1, -1)
      } else {
        val filteredHumidity = humidity.filter(_ != -1)
        LineStat(sensorId, filteredHumidity.min, filteredHumidity.sum / filteredHumidity.size, filteredHumidity.max)
      }
    }
  }

  def counter[A]: Sink[A, Future[Int]] = Sink.fold[Int, A](0)((count, _) => count + 1)

  val filesProcessing: String => SourceFiles = (directory: String) => directorySource(directory)
    .filterNot(_.endsWith(".csv"))
    .via(files)

  def countedFiles(f: String => SourceFiles)(directory: String): Future[Int] = f(directory)
    .toMat(counter[ByteString])(Keep.right)
    .run()

  def countedLines(f: String => SourceFiles)(directory: String): Future[Int] = f(directory)
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.toMapAsStrings())
    .via(flowHeader)
    .map { l =>
      ByteString(l.toString())
    }
    .toMat(counter[ByteString])(Keep.right)
    .run()

  def countedNan(f: String => SourceFiles)(directory: String): Future[Int] = f(directory)
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.toMapAsStrings())
    .via(flowHeader)
    .via(flowMap)
    .filter(_.values.toSeq.head == -1)
    .map(_.values.toSeq.head)
    .toMat(counter[Int])(Keep.right)
    .run()

  def linesStats(f: String => SourceFiles)(directory: String): Future[Seq[Set[LineStat]]] = f(directory)
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.toMapAsStrings())
    .via(flowHeader)
    .via(flowMap)
    .via(foldFlowInt)
    .via(flowStats)
    .toMat(Sink.seq)(Keep.right)
    .run()
}
