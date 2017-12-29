package com.singularities.spark.utils

import org.apache.spark.sql.{DataFrame, Row}

object CustomVisualization {

  def tail(df: DataFrame): Row ={
    df.take(df.count().toInt).last
  }

  def countNulls(df: DataFrame, col:String): Int ={
    df.filter(df(col).isNull || df(col) === "" || df(col).isNaN).count().toInt
  }

  def checkForNulls(df: DataFrame, sqlContext: org.apache.spark.sql.SQLContext): DataFrame ={
    import sqlContext.implicits._
    (for(col <- df.columns.toList) yield(col, countNulls(df, col))).toDF("column_name", "num_nulls")
  }

  def levels(df: DataFrame, column:String, numLevels:Int) = {
    df.select(column).distinct.show(numLevels, false)
  }
}
