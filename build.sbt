name := "spark-bi5"

version := "0.1"

scalaVersion := "2.11.12"

val scalaTestVersion = "3.1.1"

val sparkVersion = "2.3.1"


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.tukaani" % "xz" % "1.8"
)


 artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
   artifact.name + "-" + module.revision +  "_" + sv.binary + "_" + sparkVersion + "." + artifact.extension
 }

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}_${scalaVersion.value}_$sparkVersion.jar"
