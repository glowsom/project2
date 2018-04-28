name := "proj2"

version := "0.1"

organization := "org.apache"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-annotations" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-hadoop2-compat" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.htrace" % "htrace-core" % "3.2.0-incubating"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.6" pomOnly()

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/spark-hbase/spark-hbase</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:spark-hbase/spark-hbase.git</url>
    <connection>scm:git:git@github.com:spark-hbase/spark-hbase.git</connection>
  </scm>
  <developers>
    <developer>
      <id>haosdent</id>
      <name>Haosong Huang</name>
      <url>https://github.com/haosdent</url>
    </developer>
  </developers>)

// Enable Junit testing.
// libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"


resolvers ++= Seq{
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/"
}
