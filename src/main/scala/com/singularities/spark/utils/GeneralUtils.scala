package com.singularities.spark.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GeneralUtils {

  /**
    * Writes a DataFrame in a single CSV file.
    * @param df The dataframe to be written
    * @param header Boolean indicating if the dataframe has a header
    * @param path Path indicating where the file will be written
    */
  def writeCSV(df: DataFrame, header: String, path: String): Unit ={
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", header)
      .save(path)
  }


  def getSquareRootBins(df: DataFrame, column: String): Double ={
    Math.sqrt(df.count())
  }

  def getFreedmanDiaconisBins(df: DataFrame, column: String): Double ={
    import org.apache.commons.math3.stat.descriptive._

    val columnArray = df.select(column).rdd.map(row => row(0).asInstanceOf[Double]).collect()
    val arrMean = new DescriptiveStatistics()
    genericArrayOps(columnArray).foreach(v => arrMean.addValue(v))

    val q1 = arrMean.getPercentile(25)
    val q3 = arrMean.getPercentile(75)
    val iqr = q3 - q1

    2*iqr*Math.pow(df.count(), -1/3)
  }

  def getRiceRuleBins(df: DataFrame, column: String): Double ={
    Math.ceil(2 * Math.pow(df.count(), 1/3))
  }

  def getSturgesBins(df: DataFrame, column: String): Double ={
    (3.322 * Math.ceil(Math.log(df.count())/Math.log(2)))+1
  }

  def getScottBins(df: DataFrame, column: String): Double ={
    val deviation = df.select(stddev_pop(df(column))).collect().last.getDouble(0)
    (3.5*deviation)/Math.pow(df.count(), 1/3)
  }

  /**
    * Calculates two arrays: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    * @param df Input DataFrame
    * @param column The column to be used to generate the histogram
    * @param bins The specific number of bins to be used to generate the histogram.
    * @return
    */
  def histogramGivenBins(df: DataFrame, column: String, bins:Int) = {
    import df.sparkSession.implicits._
    df.select(column).map(value => value.getDouble(0)).rdd.histogram(bins)
  }

  /**
    * Calculates two arrays: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    * @param df Input DataFrame
    * @param column The column to be used to generate the histogram
    * @param strategy The strategy to be used to generate the histogram.
    *                 The options are: Scott, Freedman-Diaconis, Square-root and Rice-rule.
    *                 If the argument doesn't match with any of those, the default is Sturges.
    * @return The two arrays containing the histogram information.
    */
  def histogram(df: DataFrame, column: String, strategy:String) = {
    var bins = 0.0;
    strategy match {
        case "Scott"=> bins = getScottBins(df, column)
        case "Freedman-Diaconis" => bins = getFreedmanDiaconisBins(df, column)
        case "Square-root" => bins =  getSquareRootBins(df, column)
        case "Rice-rule" => bins = getRiceRuleBins(df, column)
        case _ => { bins = getSturgesBins(df, column)}
    }
    histogramGivenBins(df, column, bins.toInt)
  }
}
