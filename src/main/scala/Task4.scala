

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import CsvStats.{countedFiles, countedLines, countedNan, linesStats, filesProcessing}

object Task4 extends App {
  val directory = "/tmp/ramdisk/documents/"

  countedFiles(filesProcessing)(directory).onComplete {
    case Success(v) => println(s"Num of processed files: $v")
    case Failure(e) => println(e.getMessage)
  }

  countedLines(filesProcessing)(directory).onComplete {
    case Success(v) => println(s"Num of processed measurements: $v")
    case Failure(e) => println(e.getMessage)
  }

  countedNan(filesProcessing)(directory).onComplete {
    case Success(v) => println(s"Num of failed measurements: $v")
    case Failure(e) => println(e.getMessage)
  }

  linesStats(filesProcessing)(directory).onComplete {
    case Success(v) =>
      println("sensor-id,min,avg,max")
      v.head.toList.sorted.foreach(println)
    case Failure(e) => println(e)
  }
}
