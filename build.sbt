name := "TSPARK2"
version := "0.1"
scalaVersion := "2.12.10"

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"

libraryDependencies ++= Seq(
  "org.backuity.clist" %% "clist-core" % "3.5.1",
  "org.backuity.clist" %% "clist-macros" % "3.5.1" % "provided")


// https://mvnrepository.com/artifact/org.roaringbitmap/RoaringBitmap
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.9.0"


mainClass in assembly := Some("cmdlineparser.TSPARK")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}