/*
 * Copyright 2016 CGnal S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cgnal.spark.opentsdb

import java.io.{ BufferedWriter, File, FileWriter }
import java.nio.file._
import java.nio.file.attribute.PosixFilePermission

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool }
import org.apache.commons.pool2.{ BasePooledObjectFactory, PooledObject }
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import shaded.org.hbase.async.HBaseClient

import scala.collection.convert.decorateAsJava._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TSDBClientFactory extends BasePooledObjectFactory[TSDB] {

  @transient lazy private val log = Logger.getLogger(getClass.getName)

  override def wrap(tsdb: TSDB): PooledObject[TSDB] = new DefaultPooledObject[TSDB](tsdb)

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  override def create(): TSDB = synchronized {
    log.info("About to create the TSDB client instance")
    val hbaseClient = new HBaseClient(TSDBClientManager.asyncConfig_.getOrElse(throw new Exception("no configuration available")))
    val tsdb = new TSDB(hbaseClient, TSDBClientManager.config_.getOrElse(throw new Exception("no configuration available")))
    log.info("About to create the TSDB client instance: done")
    tsdb
  }

  override def destroyObject(pooledTsdb: PooledObject[TSDB]): Unit = synchronized {
    log.info("About to shutdown the TSDB client instance")
    pooledTsdb.getObject.shutdown().joinUninterruptibly()
    log.info("About to shutdown the TSDB client instance: done")
  }

  override def passivateObject(pooledTsdb: PooledObject[TSDB]): Unit = {
    log.info("About to flush the TSDB client instance")
    val tsdb: TSDB = pooledTsdb.getObject
    if (tsdb.getConfig.getBoolean("tsd.core.preload_uid_cache"))
      log.error(s"Cache stats: Misses=${tsdb.uidCacheMisses()} Hits=${tsdb.uidCacheHits()} Size=${tsdb.uidCacheSize()}")
    tsdb.flush().joinUninterruptibly()
    log.info("About to flush the TSDB client instance: done")
  }

}

/**
 * This class is responsible for creating and managing a TSDB client instance
 */
object TSDBClientManager {

  @transient lazy private val log = Logger.getLogger(getClass.getName)

  @transient val pool: GenericObjectPool[TSDB] = new GenericObjectPool[TSDB](new TSDBClientFactory())

  @inline private def writeStringToFile(file: File, str: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(str)
    bw.close()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var config_ : Option[Config] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var asyncConfig_ : Option[shaded.org.hbase.async.Config] = None

  /**
   *
   * @param keytabData                the keytab path
   * @param principal                 the principal
   * @param baseConf                  the configuration base used by this spark context
   * @param tsdbTable                 the tsdb table
   * @param tsdbUidTable              the tsdb-uid table
   * @param autoCreateMetrics         the metrics auto create flag
   * @param saltWidth                 the salting prefix size
   * @param saltBuckets               the number of buckets
   * @param metricWidth               the size in bytes for the metric uids
   * @param tagkWidth                 the size in bytes for the tagv uids
   * @param tagvWidth                 the size in bytes for the tagv uids
   * @param preloadUidCache           true if the uids should be cached
   * @param preloadUidCacheMaxEntries the maximun number of cache entries
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Throw"))
  def init(
    keytabLocalTempDir: Option[String],
    keytabData: Option[Broadcast[Array[Byte]]],
    principal: Option[String],
    baseConf: Configuration,
    tsdbTable: String,
    tsdbUidTable: String,
    autoCreateMetrics: Boolean,
    saltWidth: Int,
    saltBuckets: Int,
    metricWidth: Int,
    tagkWidth: Int,
    tagvWidth: Int,
    preloadUidCache: Boolean,
    preloadUidCacheMaxEntries: Int
  ): Unit = synchronized {
    if (config_.isEmpty || asyncConfig_.isEmpty) {
      log.info("Initialising the OpenTSDBClientManager")
      val configuration: Configuration = baseConf
      val authenticationType = configuration.get("hbase.security.authentication")
      val quorum = configuration.get("hbase.zookeeper.quorum")
      val port = configuration.get("hbase.zookeeper.property.clientPort")
      val asyncConfig = new shaded.org.hbase.async.Config()
      val config = new Config(false)
      config.overrideConfig("tsd.storage.hbase.data_table", tsdbTable)
      config.overrideConfig("tsd.storage.hbase.uid_table", tsdbUidTable)
      config.overrideConfig("tsd.core.auto_create_metrics", autoCreateMetrics.toString)
      config.overrideConfig("tsd.storage.uid.width.metric", metricWidth.toString)
      config.overrideConfig("tsd.storage.uid.width.tagk", tagkWidth.toString)
      config.overrideConfig("tsd.storage.uid.width.tagk", tagvWidth.toString)
      config.overrideConfig("tsd.core.preload_uid_cache", preloadUidCache.toString)
      config.overrideConfig("tsd.core.preload_uid_cache.max_entries", preloadUidCacheMaxEntries.toString)
      if (saltWidth > 0) {
        config.overrideConfig("tsd.storage.salt.width", saltWidth.toString)
        config.overrideConfig("tsd.storage.salt.buckets", saltBuckets.toString)
      }
      config.disableCompactions()
      asyncConfig.overrideConfig("hbase.zookeeper.quorum", quorum.split(",").toList.map(tk => s"$tk:$port").mkString(","))
      asyncConfig.overrideConfig("hbase.zookeeper.znode.parent", "/hbase")
      if (authenticationType == "kerberos") {
        val kdir = keytabLocalTempDir.getOrElse(throw new Exception("keytab temp dir not available"))
        try {
          Files.createDirectories(Paths.get(kdir))
        } catch {
          case _: FileAlreadyExistsException =>
        }
        val keytabPath = s"$kdir/keytab"
        val byteArray = keytabData.getOrElse(throw new Exception("keytab data not available")).value

        Files.deleteIfExists(Paths.get(keytabPath))
        val keytabFile = Files.createFile(Paths.get(s"$keytabPath"))
        Files.setPosixFilePermissions(keytabFile, Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava)
        Files.write(Paths.get(keytabPath), byteArray)

        Files.deleteIfExists(Paths.get(s"$kdir/jaas.conf"))
        val jaasFile = Files.createFile(FileSystems.getDefault.getPath(s"$kdir/jaas.conf"))
        Files.setPosixFilePermissions(jaasFile, Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava)
        val jaasConf =
          s"""AsynchbaseClient {
             |  com.sun.security.auth.module.Krb5LoginModule required
             |  useTicketCache=false
             |  useKeyTab=true
             |  keyTab="$keytabPath"
             |  principal="${principal.getOrElse(throw new Exception("principal not available"))}"
             |  storeKey=true;
             | };
        """.stripMargin
        writeStringToFile(
          jaasFile.toFile,
          jaasConf
        )

        configuration.set("hadoop.security.authentication", "kerberos")
        asyncConfig.overrideConfig("hbase.security.auth.enable", "true")
        asyncConfig.overrideConfig("hbase.security.authentication", "kerberos")
        asyncConfig.overrideConfig("hbase.kerberos.regionserver.principal", configuration.get("hbase.regionserver.kerberos.principal"))
        asyncConfig.overrideConfig("hbase.sasl.clientconfig", "AsynchbaseClient")
        asyncConfig.overrideConfig("hbase.rpc.protection", configuration.get("hbase.rpc.protection"))
        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
          override def run(): Unit = {
            Files.deleteIfExists(keytabFile)
            Files.deleteIfExists(jaasFile)
            try {
              Files.deleteIfExists(Paths.get(kdir))
            } catch {
              case _: FileSystemException =>
            }
            ()
          }
        }))
      }
      config_ = Some(config)
      asyncConfig_ = Some(asyncConfig)
      log.info("Initialising the OpenTSDBClientManager: done")
    }
  }

  def stop(): Unit = pool.close()

}