package org.meenzoon.spark.utils

object NumUdf {
  def numAdd(num1: String, num2: String): Int = {
    num1.toInt + num2.toInt
  }
}
