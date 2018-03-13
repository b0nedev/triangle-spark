name := "test_scala"

version := "0.1"

scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-sql_2.10" % "1.6.0",
)


        