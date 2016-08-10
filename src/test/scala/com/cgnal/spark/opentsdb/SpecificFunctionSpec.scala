package com.cgnal.spark.opentsdb

import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

/**
  * Created by ceppelli on 09/08/16.
  */
class SpecificFunctionSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  "processQuantifier" must {
    val tests = List(
      ("00-00", List((0, true, 1))),
      ("00-0F", List((0, false, 8))),
      ("00-0b", List((0, false, 4))),
      ("00-1b", List((1, false, 4))),
      ("00-2b", List((2, false, 4))),
      ("00-3b", List((3, false, 4))),
      ("00-4b", List((4, false, 4))),
      ("00-5b", List((5, false, 4))),
      ("00-6b", List((6, false, 4))),
      ("00-7b", List((7, false, 4))),
      ("00-8b", List((8, false, 4))),
      ("00-9b", List((9, false, 4))),
      ("00-F0", List((15, true, 1))),
      ("01-00", List((16, true, 1))),
      ("01-F0", List((31, true, 1))),
      ("0F-F0", List((255, true, 1))),
      ("F0-00-00-00", List((0, true, 1))),
      ("F0-00-00-0F", List((0, false, 8))),
      ("F0-00-00-07", List((0, true, 8))),
      ("F0-00-00-01", List((0, true, 2))),
      ("F0-00-00-00", List((0, true, 1))),
      ("F0-00-00-40", List((1, true, 1))),
      ("F0-00-00-80", List((2, true, 1))),
      ("F0-00-00-C0", List((3, true, 1))),
      ("F0-00-01-00", List((4, true, 1))),
      ("F0-00-01-40", List((5, true, 1))),
      ("F0-00-FF-00", List((1020, true, 1))),
      ("F0-00-FF-C0", List((1023, true, 1))),
      ("F0-FF-FF-C0", List((262143, true, 1))),
      ("FF-FF-FF-C0", List((4194303, true, 1)))
    )

    "check test data with the old implementation" in {
      for (i <- tests) {
        val (hex, code) = i
        val result = processQuantifierOld(hexStringToByteArray(hex))
        result must be(code)
      }
    }

    "no regression test" in {
      for (i <- tests) {
        val (hex, code) = i
        val result = processQuantifier(hexStringToByteArray(hex))
        result must be(code)
      }
    }
  }

  def hexStringToByteArray(hex: String): Array[Byte] = {
    hex.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }

}
