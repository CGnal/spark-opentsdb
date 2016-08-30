/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

import de.heikoseeberger.sbtheader.HeaderPattern
import sbt.{ExclusionRule, _}

organization := "com.cgnal.spark"

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
  "-Ywarn-dead-code",
  "-Xfuture"
)

scalacOptions in (Compile, doc) ++= Seq(
  "-no-link-warnings" // Suppresses problems with Scaladoc links
)

wartremoverErrors ++= Seq(
  Wart.Any,
  Wart.Any2StringAdd,
  //Wart.AsInstanceOf,
  //Wart.DefaultArguments,
  Wart.EitherProjectionPartial,
  Wart.Enumeration,
  //Wart.Equals,
  Wart.ExplicitImplicitTypes,
  Wart.FinalCaseClass,
  Wart.FinalVal,
  Wart.ImplicitConversion,
  Wart.IsInstanceOf,
  Wart.JavaConversions,
  Wart.LeakingSealed,
  Wart.ListOps,
  Wart.MutableDataStructures,
  //Wart.NoNeedForMonad,
  //Wart.NonUnitStatements,
  //Wart.Nothing,
  //Wart.Null,
  Wart.Option2Iterable,
  Wart.OptionPartial,
  //Wart.Overloading,
  Wart.Product,
  Wart.Return,
  Wart.Serializable,
  //Wart.Throw,
  Wart.ToString,
  Wart.TryPartial,
  Wart.Var,
  Wart.While
)

val sparkVersion = "1.6.0-cdh5.7.1"

val hadoopVersion = "2.6.0-cdh5.7.1"

val hbaseVersion = "1.2.0-cdh5.7.1"

val sparkAvroVersion = "1.1.0-cdh5.7.1"

val scalaTestVersion = "3.0.0"

val openTSDBVersion = "2.2.0"

val sparkTSVersion = "0.3.0"

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.sonatypeRepo("public"),
  Resolver.typesafeRepo("releases"),
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
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
  sparkExcludes("org.apache.hbase" % "hbase-spark" % hbaseVersion % "compile"),
  sparkExcludes("com.cloudera.sparkts" % "sparkts" % sparkTSVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-client" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-protocol" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % "compile"),
  hbaseExcludes("org.apache.hbase" % "hbase-common" % hbaseVersion % "compile"),
  "net.opentsdb" % "opentsdb-shaded" % openTSDBVersion % "compile"
)

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

val scope = "provided"

libraryDependencies ++= Seq(
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % scope),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % scope),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % scope),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % scope),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % scope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % scope)
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

val header1 = (
  HeaderPattern.cStyleBlockComment,
  """|/*
    | * Copyright 2016 CGnal S.p.A.
    | *
    | */
    |
    |""".stripMargin
  )

val header2 = (
  HeaderPattern.hashLineComment,
  """|#
    |# Copyright 2016 CGnal S.p.A.
    |#
    |#
    |
    |""".stripMargin
  )

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
      "net.opentsdb" % "opentsdb-shaded" % openTSDBVersion % "it,test"
    ),
    headers := Map(
      "sbt" -> header1,
      "scala" -> header1,
      "conf" -> header2,
      "properties" -> header2
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
    libraryDependencies in assembly := assemblyDependencies
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark")))
  })

val buildShadedLibraries = taskKey[Unit]("Build the shaded library")

buildShadedLibraries := Process("mvn" :: "install" :: Nil, new File("shaded_libraries")).!

buildShadedLibraries <<= buildShadedLibraries dependsOn buildShadedLibraries
//compile in Compile <<= compile in Compile dependsOn buildShadedLibraries  //Uncomment this if you want to rebuild the shahded libraries every time you compile/test the project
