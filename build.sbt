name := "TSPARK2"
version := "0.1"
scalaVersion := "2.12.10"

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"

mainClass in assembly := Some("cmdline.MainConsole")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}