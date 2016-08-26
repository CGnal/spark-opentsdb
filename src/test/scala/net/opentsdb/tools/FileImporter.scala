/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package net.opentsdb.tools

import net.opentsdb.core.TSDB
import shaded.org.hbase.async.HBaseClient

object FileImporter {
  def importFile(
    hbaseClient: HBaseClient,
    tsdb: TSDB,
    path: String,
    skip_errors: Boolean
  ): Int = {
    val method = classOf[TextImporter].getDeclaredMethod(
      "importFile",
      classOf[HBaseClient],
      classOf[TSDB],
      classOf[java.lang.String],
      classOf[Boolean]
    )
    method.setAccessible(true)
    val result: AnyRef = method.invoke(null, hbaseClient, tsdb, path, skip_errors.asInstanceOf[AnyRef])
    result.asInstanceOf[Int]
  }
}
