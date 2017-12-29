package com.singularities.spark.utils

import org.apache.spark.sql.{DataFrame, Row}

object DataFrameExploration {

  /**
    * Shows the number of nulls in each column
    * @param df
    * @return
    */
  def checkForNulls(df: DataFrame): DataFrame ={
    import df.sparkSession.implicits._
    (for(col <- df.columns.toList) yield(col, countNulls(df, col))).toDF("column_name", "num_nulls")
  }

  private def countNulls(df: DataFrame, col:String): Int = {
    df.filter(df(col).isNull || df(col) === "" || df(col).isNaN).count().toInt
  }

  /**
    * Shows the distinct values of a column
    * @param df
    * @param column
    * @param numLevels
    */
  def levels(df: DataFrame, column:String, numLevels:Int = 50) = {
    df.select(column).distinct.show(numLevels, false)
  }

  /**
    * Calculates two arrays: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    *
    * @param df       Input DataFrame
    * @param column   The column to be used to generate the histogram
    * @param strategy The strategy to be used to generate the histogram.
    *                 The options are: Scott, Freedman-Diaconis, Square-root and Rice-rule.
    *                 If the argument doesn't match with any of those, the default is Sturges.
    * @return The two arrays containing the histogram information.
    */
  def histogram(df: DataFrame, column: String, strategy: BucketStrategy.Value = BucketStrategy.STURGES,numBins:Int=0): (Array[Double], Array[Long]) = {
    val hg = if(numBins !=0) {
      new HistogramGenerator(df,column,strategy,numBins)
    } else {
      new HistogramGenerator(df,column,strategy)
    }
    hg.generate()
  }

  /**
    * Calculates a DataFrame with two columns: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    *
    * @param df       Input DataFrame
    * @param column   The column to be used to generate the histogram
    * @param strategy The strategy to be used to generate the histogram.
    *                 The options are: Scott, Freedman-Diaconis, Square-root and Rice-rule.
    *                 If the argument doesn't match with any of those, the default is Sturges.
    * @return The DataFrame containing the histogram information.
    */
  def histogramDF(df: DataFrame, column: String, strategy: BucketStrategy.Value = BucketStrategy.STURGES,numBins:Int=0): DataFrame = {
    val hg = if(numBins !=0) {
      new HistogramGenerator(df,column,strategy,numBins)
    } else {
      new HistogramGenerator(df,column,strategy)
    }
    hg.generateDF()
  }
}
