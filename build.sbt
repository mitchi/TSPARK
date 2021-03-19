name := "TSPARK2"
version := "0.2"
scalaVersion := "2.12.10"

//scalacOptions ++= Seq(
//  "-opt:l:method",
//  "-opt-warnings:at-inline-failed-summary",
//  "-verbose",
//)

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0"

libraryDependencies ++= Seq(
  "org.backuity.clist" %% "clist-core" % "3.5.1",
  "org.backuity.clist" %% "clist-macros" % "3.5.1" % "provided")


// https://mvnrepository.com/artifact/org.roaringbitmap/RoaringBitmap
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.9.0"

//Ajout du parser d'expressions booleenes
//libraryDependencies += "com.hypertino" %% "expression-parser" % "0.3.0"
//libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.2.2"

mainClass in assembly := Some("cmdlineparser.TSPARK")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}