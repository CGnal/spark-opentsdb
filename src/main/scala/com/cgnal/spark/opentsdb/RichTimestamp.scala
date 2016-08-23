package com.cgnal.spark.opentsdb

import java.sql.Timestamp

final class RichTimestamp(val self: Timestamp) extends AnyVal {
  def ->(end: Timestamp) = Some((
    (self.getTime / 1000).toInt,
    (end.getTime / 1000).toInt
    ))
}