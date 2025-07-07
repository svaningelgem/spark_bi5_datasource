lazy val commonSettings = Seq(
  name         := "spark-bi5",
  version      := "0.1",
  scalaVersion := "2.11.12",

  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.1.1" % "test",
    "org.tukaani" % "xz" % "1.10"
  )
)

lazy val common = project
  .settings(
    commonSettings
  )

lazy val spark_2_3 = (project in file("spark-2.3"))
  .settings(commonSettings)
  .dependsOn(common)

lazy val spark_2_4 = (project in file("spark-2.4"))
  .settings(commonSettings)
  .dependsOn(common)
