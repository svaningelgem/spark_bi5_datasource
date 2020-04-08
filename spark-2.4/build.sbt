val sparkVersion = "2.4.5"


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.tukaani" % "xz" % "1.8"
)


assemblyJarName in assembly := s"${name.value}-${version.value}_${scalaVersion.value}_$sparkVersion.jar"
