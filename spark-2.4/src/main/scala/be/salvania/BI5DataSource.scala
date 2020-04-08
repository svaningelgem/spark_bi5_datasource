package be.salvania

// Found help at:
// https://github.com/assafmendelson/DataSourceV2/tree/master/src/main/scala/com/example/sources/readers/trivial

import java.io.{DataInputStream, EOFException, File, FileInputStream}
import java.nio.file.{FileVisitOption, Files, Path, Paths}
import java.sql.Timestamp
import java.util
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.tukaani.xz.LZMAInputStream

import scala.collection.JavaConverters._
import scala.util.matching.Regex


class BI5DataSource
  extends DataSourceRegister
    with DataSourceV2
    with ReadSupport {

  override def shortName(): String = "bi5"

  override def createReader(options: DataSourceOptions): DataSourceReader = { // to implement ReadSupport
    val parameters = options.asMap().asScala // converts options to lower-cased keyed map

    val path = parameters.getOrElse("path", throw new IllegalArgumentException("'path' must be specified for BI5 data."))
    if ( !Files.exists(Paths.get(path)) ) {
      throw new IllegalArgumentException("Invalid path")
    }

    val digits: Int = parameters.getOrElse("digits", throw new IllegalArgumentException("'digits' should be the digits for the currency")).toInt
    if (digits < 0) {
      throw new IllegalArgumentException("digits cannot be smaller than 0")
    }

    val january_starts_at_0: Int = parameters.getOrElse("january", "0").toInt
    if (january_starts_at_0 < 0 || january_starts_at_0 > 1 ) {
      throw new IllegalArgumentException("january can only be 0 or 1")
    }

    new BI5BatchDataSourceReader(path, digits, january_starts_at_0)
  }
}


class BI5BatchDataSourceReader(path: String, digits: Int, january_starts_at_0: Int)
  extends DataSourceReader {

  override def readSchema(): StructType = {
    StructType(Seq(
      StructField("ticker", StringType, nullable = false),
      StructField("ts", TimestampType, nullable = false),
      StructField("ask", DoubleType, nullable = false),
      StructField("bid", DoubleType, nullable = false),
      StructField("ask_volume", DoubleType, nullable = false),
      StructField("bid_volume", DoubleType, nullable = false)
    ))
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val p = Paths.get(path)
    if (Files.isDirectory(p)) { // Only loop over the immediate directories. We'll handle the extra looping down in the Reader
      new File(path).list().map(subdir =>
          new BI5BatchDataSourcePartition(p.resolve(subdir).toString, digits, january_starts_at_0).asInstanceOf[InputPartition[InternalRow]]
      ).toSeq.asJava
    }
    else { // We already did our path check in the @BI5DataSource
      // It's just 1 file...
      Seq(new BI5BatchDataSourcePartition(path, digits, january_starts_at_0).asInstanceOf[InputPartition[InternalRow]]).asJava
    }
  }
}


class BI5BatchDataSourcePartition(path: String, digits: Int, january_starts_at_0: Int)
  extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new BI5BatchDataReader(path, digits, january_starts_at_0)
  }
}


class BI5BatchDataReader(path: String, digits: Int, january_starts_at_0: Int)
  extends InputPartitionReader[InternalRow] {

  lazy private val pattern = new Regex(
    "/([a-zA-Z0-9]+)/(\\d{4})/(\\d{1,2})/(\\d{1,2})/(\\d{1,2})h_ticks.bi5$",
    "ticker",      "year",  "month",   "day",     "hour"
  )

  private val divisor = scala.math.pow(10, digits)

  // https://stackoverflow.com/questions/2637643/how-do-i-list-all-files-in-a-subdirectory-in-scala
  lazy private val currentPath: Iterator[Path] = {
    Files
      .walk(Paths.get(path), FileVisitOption.FOLLOW_LINKS)
      .iterator()
      .asScala
      .filter(_.toString.toLowerCase.endsWith(".bi5"))
  }
  private var currentFile: String = _
  private var currentDate: Long = _
  private var currentTicker: UTF8String = _

  private var in: DataInputStream = _

  private def _setupCurrents(filename: String): Unit = {
    currentFile = filename

    val _match = pattern.findFirstMatchIn(
      currentFile.toString.replace('\\', '/')
    ).getOrElse(
      throw new IllegalArgumentException("Invalid path provided. Should be in the format <currency>/<YYYY>/<mm>/<dd>/<hh>h_ticks.bi5")
    )

    currentTicker = UTF8String.fromString(_match.group("ticker"))

    var currentMonth: Int = _match.group("month").toInt
    if (january_starts_at_0 == 1)
      currentMonth = currentMonth - 1

    val dt = new Calendar.Builder()
      .setFields(
        Calendar.YEAR, _match.group("year").toInt,
        Calendar.MONTH, currentMonth,
        Calendar.DAY_OF_MONTH, _match.group("day").toInt,
        Calendar.HOUR_OF_DAY, _match.group("hour").toInt
      )
      .setTimeZone(TimeZone.getTimeZone("UTC"))
      .build()

    currentDate = dt.getTimeInMillis * 1000L

    in = new DataInputStream(new LZMAInputStream(new FileInputStream(currentFile)))
  }

  private var hasNext: Boolean = true
  private var nextRow: InternalRow = getNextRow

  @scala.annotation.tailrec
  private def getNextRow: InternalRow = {
    while (null == in && currentPath.hasNext) {
      try {
        _setupCurrents(currentPath.next().toAbsolutePath.toString)
      }
      catch {
        // Silently ignore all exceptions thrown
        case _: Throwable => in = null
      }
    }

    if (null == in) {
      hasNext = false
      return null
    }

    try {
      val milliseconds = in.readInt()
      val ask: Double = in.readInt() / divisor
      val bid: Double = in.readInt() / divisor
      val askV: Double = in.readFloat()
      val bidV: Double = in.readFloat()

      val newDate = currentDate + milliseconds * 1000L

      return InternalRow(currentTicker, newDate, ask, bid, askV, bidV)
    } catch {
            // I just want to show it here, but basically it means: eat any exception thrown
      case _: EOFException =>
      case _: Throwable =>
        // Silently ignore the rest
    }

    close()

    getNextRow
  }

  override def next(): Boolean = hasNext

  override def get(): InternalRow = {
    val row_to_send = nextRow
    nextRow = getNextRow
    row_to_send
  }

  override def close(): Unit = {
    if (null != in) {
      in.close()
      in = null
    }
  }
}
