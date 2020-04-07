# BI5 import into a Spark dataframe

# Motivation
I have a whole lot of Dukascopy's RAW data files. Recently I took up Spark learning and as such I wanted an easy way to load them all.

This spark data source can load bi5 files one by one, or you can load them all at once by pointing to a directory.

*Only loading is implemented. No writing. If you want, you can always save your DataFrame into a Parquet file; that's more efficient anyway for further processing.*

The expected directory structure is as in test/resources:
```
<ticker symbol>/<YYYY>/<mm>/<dd>/<hh>h_ticks.bi5
```
The month is a bit special. How I download it, is that january == 0. This is the default as well. But some people might want to store it with their real numbers (so january = 1). There's an option for that (see below).

# Options

Parameter | Optional/Required | Default | Remark
---|---|---|---
path | Required | --- | See below for a bit more explanation
digits | Required | --- | The amount of digits after the decimal (for example EURUSD = 5, USDJPY = 3)
january | Optional | `0` | Which number corresponds to the first month in your directory structure? `0` or `1`

# Path
The reader has been set up so you can ór read in single files, ór you can just path the root directory to the ticker symbol and it will load up ALL the files in the directory structure.

Files that are broken or not the right filename are silently ignored.

# Build and Test

The project can be build using `sbt assembly` and tested using spark-shell as follows:

## Example
```
spark-shell --jars target/scala-2.11/spark-bi5-0.1_2.11.12_2.3.1.jar
....

val mydata1 = spark.read.format("bi5").option("digits", 5).load("EURUSD/")
mydata1.printSchema
mydata1.show(10, false)

val mydata2 = spark.read.format("bi5").option("digits", 3).option("january", 1).load(getPath("USDJPY/"))
mydata2.show(10, false)
```