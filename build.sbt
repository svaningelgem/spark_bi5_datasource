lazy val commonSettings = Seq(
  name         := "spark-bi5",
  version      := "0.1",
  scalaVersion := "2.11.12",

  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
)


lazy val spark_2_3 = (project in file("spark-2.3"))
  .settings(commonSettings)
