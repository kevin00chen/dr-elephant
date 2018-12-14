//
// Copyright 2016 LinkedIn Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//

import sbt._

object Dependencies {

  // Dependency Version
  lazy val commonsCodecVersion = "1.10"
  lazy val commonsIoVersion = "2.4"
  lazy val gsonVersion = "2.2.4"
  lazy val guavaVersion = "18.0"          // Hadoop defaultly are using guava 11.0, might raise NoSuchMethodException
  lazy val jacksonMapperAslVersion = "1.7.3"
  lazy val jacksonVersion = "2.5.3"
  lazy val jerseyVersion = "2.24"
  lazy val jsoupVersion = "1.7.3"
  lazy val mysqlConnectorVersion = "5.1.36"
  lazy val oozieClientVersion = "4.2.0"
  lazy val scalaV = "2.11"

  lazy val HADOOP_VERSION = "hadoopversion"
  lazy val SPARK_VERSION = "sparkversion"

  var hadoopVersion = "2.3.0"
  if (System.getProperties.getProperty(HADOOP_VERSION) != null) {
    hadoopVersion = System.getProperties.getProperty(HADOOP_VERSION)
  }

  var sparkVersion = "2.1.1"
  if (System.getProperties.getProperty(SPARK_VERSION) != null) {
    sparkVersion = System.getProperties.getProperty(SPARK_VERSION)
  }

  val sparkDependency = Seq(
    if (sparkVersion >= "1.5.0") {
      "org.apache.spark" % s"spark-core_$scalaV" % sparkVersion excludeAll(
        ExclusionRule(organization = "com.typesafe.akka"),
        ExclusionRule(organization = "org.apache.avro"),
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.glassfish.hk2", name = "hk2-locator"),
        ExclusionRule(organization = "net.razorvine")
        )
    } else {
      "org.apache.spark" % s"spark-core_$scalaV" % sparkVersion excludeAll(
        ExclusionRule(organization = "org.apache.avro"),
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "net.razorvine")
        )
    },
    "org.apache.spark" % s"spark-streaming_$scalaV" % sparkVersion,
    "org.apache.spark" % s"spark-sql_$scalaV" % sparkVersion,
    "org.apache.spark" % s"spark-streaming-kafka-0-8_$scalaV" % sparkVersion
  )

  // Dependency coordinates
  var requiredDep = Seq(
    "com.google.code.gson" % "gson" % gsonVersion,
    "com.google.guava" % "guava" % guavaVersion,
    "com.jsuereth" %% "scala-arm" % "1.4",
    "commons-codec" % "commons-codec" % commonsCodecVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "javax.ws.rs" % "javax.ws.rs-api" % "2.0.1",
    "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
    "org.apache.hadoop" % "hadoop-auth" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % Test,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test,
    "org.jsoup" % "jsoup" % jsoupVersion,
    "org.apache.oozie" % "oozie-client" % oozieClientVersion excludeAll(
      ExclusionRule(organization = "org.apache.hadoop")
      ),
    "org.glassfish.jersey.core" % "jersey-client" % jerseyVersion,
    "org.glassfish.jersey.core" % "jersey-common" % jerseyVersion,
    "org.glassfish.jersey.media" % "jersey-media-json-jackson" % jerseyVersion % Test,
    "org.glassfish.jersey.test-framework" % "jersey-test-framework-core" % jerseyVersion % Test,
    "org.glassfish.jersey.test-framework.providers" % "jersey-test-framework-provider-grizzly2" % jerseyVersion % Test,
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.4",
    //    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
    "io.dropwizard.metrics" % "metrics-healthchecks" % "3.1.2",
    "org.mockito" % "mockito-core" % "1.10.19" exclude ("org.hamcrest", "hamcrest-core"),
    "org.jmockit" % "jmockit" % "1.23" % Test,
    "org.apache.httpcomponents" % "httpclient" % "4.5.2",
    "org.apache.httpcomponents" % "httpcore" % "4.4.4",
    "org.scalatest" %% "scalatest" % "3.0.0" % Test,
    "com.h2database" % "h2" % "1.4.196" % Test,
    "commons-dbutils" % "commons-dbutils" % "1.6",
    "com.mchange" % "c3p0" % "0.9.5",
    "com.alibaba" % "fastjson" % "1.2.31",
    "com.amazonaws" % "emrfs-hadoop" % "2.18.0",
    "javax.validation" % "validation-api" % "1.1.0.Final"
  ) ++ sparkDependency

  //  var dependencies = Seq(javaJdbc, javaEbean, cache)

  var dependencies = Seq(
//    "com.typesafe.play" % "play-java-ebean_2.11" % "2.3.9",
    "com.typesafe.play" % "play-java-jdbc_2.11" % "2.4.6",
    "com.typesafe.play" % "play-cache_2.11" % "2.4.6"
  )

  dependencies ++= requiredDep

  val exclusionRules = Seq(
    ExclusionRule(organization = "com.sun.jersey", name = "jersey-core"),
    ExclusionRule(organization = "com.sun.jersey", name = "jersey-server")
  )
}