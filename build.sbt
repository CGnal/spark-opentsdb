import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt.{ExclusionRule, _}

organization := "com.cgnal"

name := "spark-opentsdb"

version in ThisBuild := "1.0"

val assemblyName = "spark-opentsdb-assembly"

scalaVersion := "2.10.6"

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

scalariformSettings

scalastyleFailOnError := true

dependencyUpdatesExclusions := moduleFilter(organization = "org.scala-lang")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

wartremoverErrors ++= Seq(
  Wart.Any,
  Wart.Any2StringAdd,
  Wart.EitherProjectionPartial,
  //  Wart.OptionPartial,
  Wart.Product,
  Wart.Serializable,
  Wart.ListOps,
  Wart.Nothing
)

val sparkVersion = "1.6.0-cdh5.7.1"

val hadoopVersion = "2.6.0-cdh5.7.1"

val hbaseVersion = "1.2.0-cdh5.7.1"

val sparkAvroVersion = "1.1.0-cdh5.7.1"

val scalaTestVersion = "2.2.6"

val openTSDBVersion = "2.2.0"

val sparkTSVersion = "0.3.0"

val shapelessVersion = "2.3.1"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.mavenLocal
)

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy").
    exclude("org.apache.zookeeper", "zookeeper").
    exclude("commons-collections", "commons-collections").
    exclude("commons-beanutils", "commons-beanutils")

val hbaseExcludes =
  (moduleID: ModuleID) => moduleID.
    exclude("org.slf4j", "slf4j-log4j12").
    exclude("log4j", "log4j").
    exclude("org.apache.thrift", "thrift").
    exclude("org.jruby", "jruby-complete").
    exclude("org.slf4j", "slf4j-log4j12").
    exclude("org.mortbay.jetty", "jsp-2.1").
    exclude("org.mortbay.jetty", "jsp-api-2.1").
    exclude("org.mortbay.jetty", "servlet-api-2.5").
    exclude("com.sun.jersey", "jersey-core").
    exclude("com.sun.jersey", "jersey-json").
    exclude("com.sun.jersey", "jersey-server").
    exclude("org.mortbay.jetty", "jetty").
    exclude("org.mortbay.jetty", "jetty-util").
    exclude("tomcat", "jasper-runtime").
    exclude("tomcat", "jasper-compiler").
    exclude("org.jboss.netty", "netty").
    exclude("io.netty", "netty").
    exclude("com.google.guava", "guava").
    exclude("io.netty", "netty").
    exclude("commons-logging", "commons-logging").
    exclude("org.apache.xmlgraphics", "batik-ext").
    exclude("commons-collections", "commons-collections").
    exclude("xom", "xom").
    exclude("commons-beanutils", "commons-beanutils")

val assemblyDependencies = Seq(
  sparkExcludes("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion % "compile"),
  sparkExcludes("org.apache.hbase" % "hbase-spark" % hbaseVersion % "compile"),
  sparkExcludes("com.cloudera.sparkts" % "sparkts" % sparkTSVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-client" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-protocol" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-common" % hbaseVersion % "compile"),
  "com.chuusai" %% "shapeless" % shapelessVersion % "compile",
  "net.opentsdb" % "opentsdb" % openTSDBVersion % "compile"
    exclude("net.opentsdb", "opentsdb_gwt_theme")
    exclude("com.google.guava", "guava")
    exclude("ch.qos.logback", "*")
    exclude("com.google.gwt", "gwt-user")
    exclude("org.jboss.netty", "*")
    exclude("log4j", "*")
    exclude("org.slf4j", "*")
    exclude("org.hbase", "asynchbase"),
  "org.hbase" % "asynchbase-shaded" % "1.7.1"
    exclude("log4j", "*")
    exclude("org.slf4j", "*")
    exclude("com.google.guava", "guava")
)

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

libraryDependencies ++= Seq(
  sparkExcludes("com.databricks" %% "spark-avro" % sparkAvroVersion % "provided"),
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % "provided"),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % "provided"),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % "provided"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided")
) ++ assemblyDependencies

//http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table
fork := true

parallelExecution in Test := false

val hadoopHBaseExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-log4j12").
    exclude("javax.servlet", "servlet-api").
    excludeAll(ExclusionRule(organization = "org.mortbay.jetty")).
    excludeAll(ExclusionRule(organization = "javax.servlet"))

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      hadoopHBaseExcludes("org.scalatest" % "scalatest_2.10" % scalaTestVersion % "it,test"),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-common" % hbaseVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-testing-util" % hbaseVersion % "it,test" classifier "tests"
        exclude("org.apache.hadoop<", "hadoop-hdfs")
        exclude("org.apache.hadoop", "hadoop-minicluster")
        exclude("org.apache.hadoo", "hadoop-client")),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "it,test" classifier "tests" extra "type" -> "test-jar"),
      hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "it,test" classifier "tests" extra "type" -> "test-jar"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "it,test" classifier "tests" extra "type" -> "test-jar"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "it,test" extra "type" -> "test-jar"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % "it,test" classifier "tests"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "it,test"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-common" % hadoopVersion % "it,test" classifier "tests" extra "type" -> "test-jar"),
      hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "it,test" classifier "tests"),
      "net.opentsdb" % "opentsdb" % openTSDBVersion % "it,test"
        exclude("net.opentsdb", "opentsdb_gwt_theme")
        exclude("com.google.guava", "guava")
        exclude("ch.qos.logback", "*")
        exclude("com.google.gwt", "gwt-user")
        exclude("org.jboss.netty", "*")
        exclude("log4j", "*")
        exclude("org.slf4j", "*")
    ),
    headers := Map(
      "sbt" -> Apache2_0("2016", "CGnal S.p.A."),
      "scala" -> Apache2_0("2016", "CGnal S.p.A."),
      "conf" -> Apache2_0("2016", "CGnal S.p.A.", "#"),
      "properties" -> Apache2_0("2016", "CGnal S.p.A.", "#")
    )
  ).
  enablePlugins(AutomateHeaderPlugin).
  disablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    ivyScala := ivyScala.value map {
      _.copy(overrideScalaVersion = true)
    },
    assemblyMergeStrategy in assembly := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyJarName in assembly := s"$assemblyName-${version.value}.jar",
    libraryDependencies ++= assemblyDependencies
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark")))
  })