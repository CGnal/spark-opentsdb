/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

import java.io.{ BufferedWriter, File, FileWriter }
import java.nio.file.{ Files, Paths }

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import shaded.org.hbase.async.HBaseClient

import scala.util.{ Success, Try }

/**
 *
 */
object TSDBClientManager {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  @inline private def writeStringToFile(file: File, str: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(str)
    bw.close()
  }

  @inline private def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  def shutdown() = {
    log.trace("About to shutdown the TSDB client instance")
    tsdb_.foreach(_.map(_.shutdown().joinUninterruptibly()))
    tsdb_ = None
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var tsdb_ : Option[Try[TSDB]] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var config_ : Option[Config] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var asyncConfig_ : Option[shaded.org.hbase.async.Config] = None

  /**
   *
   * @return
   */
  def tsdb: Try[TSDB] = tsdb_.getOrElse {
    Try {
      log.trace("Creating the TSDB client instance")
      val hbaseClient = new HBaseClient(asyncConfig_.getOrElse(throw new Exception("no configuration available")))
      val tsdb = new TSDB(hbaseClient, config_.getOrElse(throw new Exception("no configuration available")))
      tsdb_ = Some(Success(tsdb))
      tsdb
    }
  }

  /**
   *
   * @param keytab  the keytab path
   * @param principal  the principal
   * @param hbaseContext the HBaseContext
   * @param tsdbTable the tsdb table
   * @param tsdbUidTable the tsdb-uid table
   * @param saltWidth the salting prefix size
   * @param saltBuckets the number of buckets
   */
  def apply(
    keytab: Option[Broadcast[Array[Byte]]],
    principal: Option[String],
    hbaseContext: HBaseContext,
    tsdbTable: String,
    tsdbUidTable: String,
    saltWidth: Int,
    saltBuckets: Int
  ): Unit = {
    val configuration: Configuration = {
      val configuration: Configuration = hbaseContext.broadcastedConf.value.value
      val authenticationType = configuration.get("hbase.security.authentication")
      if (authenticationType == null)
        HBaseConfiguration.create()
      else
        configuration
    }
    val authenticationType = configuration.get("hbase.security.authentication")
    val quorum = configuration.get("hbase.zookeeper.quorum")
    val port = configuration.get("hbase.zookeeper.property.clientPort")
    val asyncConfig = new shaded.org.hbase.async.Config()
    val config = new Config(false)
    config.overrideConfig("tsd.storage.hbase.data_table", tsdbTable)
    config.overrideConfig("tsd.storage.hbase.uid_table", tsdbUidTable)
    config.overrideConfig("tsd.core.auto_create_metrics", "true")
    if (saltWidth > 0) {
      config.overrideConfig("tsd.storage.salt.width", saltWidth.toString)
      config.overrideConfig("tsd.storage.salt.buckets", saltBuckets.toString)
    }
    config.disableCompactions()
    asyncConfig.overrideConfig("hbase.zookeeper.quorum", s"$quorum:$port")
    asyncConfig.overrideConfig("hbase.zookeeper.znode.parent", "/hbase")
    if (authenticationType == "kerberos") {
      val keytabPath = s"$getCurrentDirectory/keytab"
      val byteArray = keytab.getOrElse(throw new Exception("keytab data not available")).value
      Files.write(Paths.get(keytabPath), byteArray)
      val jaasFile = java.io.File.createTempFile("jaas", ".jaas")
      val jaasConf =
        s"""AsynchbaseClient {
            |  com.sun.security.auth.module.Krb5LoginModule required
            |  useTicketCache=false
            |  useKeyTab=true
            |  keyTab="$keytabPath"
            |  principal="${principal.getOrElse(throw new Exception("principal not available"))}"
            |  storeKey=true;
                };
            """.stripMargin
      writeStringToFile(jaasFile, jaasConf)
      System.setProperty(
        "java.security.auth.login.config",
        jaasFile.getAbsolutePath
      )
      configuration.set("hadoop.security.authentication", "kerberos")
      asyncConfig.overrideConfig("hbase.security.auth.enable", "true")
      asyncConfig.overrideConfig("hbase.security.authentication", "kerberos")
      asyncConfig.overrideConfig("hbase.kerberos.regionserver.principal", configuration.get("hbase.regionserver.kerberos.principal"))
      asyncConfig.overrideConfig("hbase.sasl.clientconfig", "AsynchbaseClient")
      asyncConfig.overrideConfig("hbase.rpc.protection", configuration.get("hbase.rpc.protection"))
      log.trace("Created kerberos configuration environment")
      log.trace(s"principal: ${principal.getOrElse(throw new Exception)}")
      log.trace(s"jaas path: ${jaasFile.getAbsolutePath}")
      log.trace(s"keytab path: $keytabPath")
    }
    config_ = Some(config)
    asyncConfig_ = Some(asyncConfig)
  }

}
