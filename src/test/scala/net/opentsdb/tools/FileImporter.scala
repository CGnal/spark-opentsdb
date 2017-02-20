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
