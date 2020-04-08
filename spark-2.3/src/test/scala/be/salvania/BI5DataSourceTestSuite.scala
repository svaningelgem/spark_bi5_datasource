package be.salvania

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite




/**
  * Created by Umberto on 06/02/2017.
  * https://gist.githubusercontent.com/umbertogriffo/112a02848d8be269f23757c9656df908/raw/5082f601e70e5ab3205049c7bab41022f14b43dd/DataFrameSuite.scala
  */
trait DataFrameSuite {
  /**
    * Compares if two [[DataFrame]]s are equal.
    * This approach correctly handles cases where the DataFrames may have duplicate rows, rows in different orders, and/or columns in different orders.
    * 1. Check two schemas are equal
    * 2. Check the number of rows are equal
    * 3. Check there is no unequal rows
    *
    * @param a         DataFrame
    * @param b         DataFrame
    * @param isRelaxed Boolean
    * @return
    */
  def areDataFramesEqual(a: DataFrame, b: DataFrame, isRelaxed: Boolean = true): Boolean = {

    try {

      a.rdd.cache
      b.rdd.cache

      // 1. Check the equality of two schemas
      if (!a.schema.toString().equalsIgnoreCase(b.schema.toString)) {
        return false
      }

      // 2. Check the number of rows in two dfs
      if (a.count() != b.count()) {
        return false
      }

      // 3. Check there is no unequal rows
      val aColumns: Array[String] = a.columns
      val bColumns: Array[String] = b.columns

      // To correctly handles cases where the DataFrames may have columns in different orders
      scala.util.Sorting.quickSort(aColumns)
      scala.util.Sorting.quickSort(bColumns)
      val aSeq: Seq[Column] = aColumns.map(col)
      val bSeq: Seq[Column] = bColumns.map(col)

      var a_prime: DataFrame = null
      var b_prime: DataFrame = null

      if (isRelaxed) {
        a_prime = a
        //            a_prime.show()
        b_prime = b
        //            a_prime.show()
      }
      else {
        // To correctly handles cases where the DataFrames may have duplicate rows and/or rows in different orders
        a_prime = a.sort(aSeq: _*).groupBy(aSeq: _*).count()
        //    a_prime.show()
        b_prime = b.sort(aSeq: _*).groupBy(bSeq: _*).count()
        //    a_prime.show()
      }

      val c1: Long = a_prime.except(b_prime).count()
      val c2: Long = b_prime.except(a_prime).count()

      if (c1 != c2 || c1 != 0 || c2 != 0) {
        return false
      }
    } finally {
      a.rdd.unpersist()
      b.rdd.unpersist()
    }

    true
  }
}


class BI5DataSourceTestSuite
  extends AnyFunSuite
  with DataFrameSuite
  with BeforeAndAfter
{
  lazy private val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
  lazy private val spark = {
    val ss = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }
  private val TEST_PATH = "EURUSD/2019/11/31/15h_ticks.bi5"
  private val TEST_PATH_WRONG_FILE = "EURUSD/2019/11/31/test.document.txt"
  private val TEST_PATH_WRONG_DATA = "EURUSD/2019/11/31/01h_ticks.bi5"
  private val DIGITS_EUR = 5
  private val DIGITS_JPY = 3
  lazy private val myBI5schema = new BI5BatchDataSourceReader("", 0, 0).readSchema()
  lazy private val FIRST_RECORD_EUR = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(Row("EURUSD", new Timestamp(dateFormat.parse("2019-12-31 15:00:00.090").getTime), 1.12207, 1.12198, 1.5, 2.25))), myBI5schema)
  lazy private val LAST_RECORD_EUR = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(Row("EURUSD", new Timestamp(dateFormat.parse("2019-12-31 15:59:59.395").getTime), 1.12240, 1.12238, 0.75, 0.1899999976158142))), myBI5schema)
  lazy private val LAST_RECORD_EUR2 = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(Row("EURUSD", new Timestamp(dateFormat.parse("2020-04-03 00:59:59.036").getTime), 1.08429, 1.08423, 3.0, 3.369999885559082))), myBI5schema)
  lazy private val FIRST_RECORD_JPY = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(Row("USDJPY", new Timestamp(dateFormat.parse("2020-12-01 23:00:00.219").getTime), 108.705, 108.677, 1.0, 1.0299999713897705))), myBI5schema)
  lazy private val LAST_RECORD_JPY = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(Row("USDJPY", new Timestamp(dateFormat.parse("2020-12-01 23:59:59.213").getTime), 108.727, 108.723, 3.25, 1.5))), myBI5schema)


  private final val loader: ClassLoader = getClass.getClassLoader
  private def getPath(path: String) : String = {
    val b = loader.getResource(path).getPath
    val c = new File(b)

    c.getAbsolutePath
  }

  private def assertDataFramesEqual(a: DataFrame, b: DataFrame): Unit = {
    if ( !areDataFramesEqual(a, b) ) {
      a.show(truncate = false)
      b.show(truncate = false)
      assert(false)
    }
  }

  private def _check_if_df_is_ok(df: DataFrame, record_count: Int = 0, partition_count: Int = 1, first_record: DataFrame = null, last_record: DataFrame = null) {
    df.cache
    assert(df.count == record_count)
    assert(df.rdd.getNumPartitions == partition_count)
    if ( null != first_record ) {
      val rec = df.orderBy("ts").limit(1)
      assertDataFramesEqual(rec, first_record)
    }
    if ( null != last_record ) {
      val rec = df.orderBy(desc("ts")).limit(1)
      assertDataFramesEqual(rec, last_record)
    }

    df.unpersist
  }


  private val myCurrentTimezone = TimeZone.getDefault
  before {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  after {
    TimeZone.setDefault(myCurrentTimezone)
  }


  test("Load bi5 without a path") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").load.count
    }
    assert(exception.getMessage === "'path' must be specified for BI5 data.")
  }

  test("Load bi5 with wrong path") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").option("digits", 1).load("bumba").count
    }
    assert(exception.getMessage === "Invalid path")
  }

  test("Load bi5 with wrong file extension") {
    val df = spark.read.format("bi5").option("digits", 1).load(getPath(TEST_PATH_WRONG_FILE))
    _check_if_df_is_ok(df)
  }

  test("Load bi5 with wrong file data") {
    val df = spark.read.format("bi5").option("digits", 1).load(getPath(TEST_PATH_WRONG_DATA))
    _check_if_df_is_ok(df)
  }

  test("Load a single bi5 file without digits") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").load(getPath(TEST_PATH)).count
    }
    assert(exception.getMessage == "'digits' should be the digits for the currency")
  }

  test("Load a single bi5 file with wrong digits") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").option("digits", -1).load(getPath(TEST_PATH)).count
    }
    assert(exception.getMessage == "digits cannot be smaller than 0")
  }

  test("Load a single bi5 file with too low january") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").option("digits", DIGITS_EUR).option("january", -1).load(getPath(TEST_PATH)).count
    }
    assert(exception.getMessage == "january can only be 0 or 1")
  }

  test("Load a single bi5 file with too high january") {
    val exception = intercept[IllegalArgumentException] {
      spark.read.format("bi5").option("digits", DIGITS_EUR).option("january", 2).load(getPath(TEST_PATH)).count
    }
    assert(exception.getMessage == "january can only be 0 or 1")
  }

  test("Load a single bi5 file correctly") {
    val df = spark.read.format("bi5").option("digits", DIGITS_EUR).load(getPath(TEST_PATH))
    _check_if_df_is_ok(df, 8816, 1, FIRST_RECORD_EUR, LAST_RECORD_EUR)
  }

  test("Load a bunch of bi5 files correctly") {
    val df = spark.read.format("bi5").option("digits", DIGITS_EUR).load(getPath("EURUSD/"))
    _check_if_df_is_ok(df, 27521, 2, FIRST_RECORD_EUR, LAST_RECORD_EUR2)
  }

  test("Load a file with january set") {
    val df = spark.read.format("bi5").option("digits", DIGITS_JPY).option("january", 1).load(getPath("USDJPY/"))
    _check_if_df_is_ok(df, 1454, 1, FIRST_RECORD_JPY, LAST_RECORD_JPY)
  }

  test("Load and write back to parquet") {
    val df = spark.read.format("bi5").option("digits", DIGITS_EUR).load(getPath("EURUSD/"))
    df.write.mode("overwrite").parquet("/tmp/output.parquet")
  }
}
