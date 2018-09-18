name := "SNIIRAM-flattening"
version := "1.0"
scalaVersion := "2.11.7"
val sparkVersion = "2.3.0"

parallelExecution in Test := false

test in assembly := {}

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "2.3.0" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.9.0"


