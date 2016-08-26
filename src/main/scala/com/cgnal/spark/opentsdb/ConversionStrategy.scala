/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

sealed trait ConversionStrategy

case object NoConversion extends ConversionStrategy

case object ConvertToDouble extends ConversionStrategy
