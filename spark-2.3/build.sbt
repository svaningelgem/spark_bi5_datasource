val sparkVersion = "4.0.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)


assemblyJarName in assembly := s"${name.value}-${version.value}_${scalaVersion.value}_$sparkVersion.jar"
