package com.singularities.spark.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.stddev_pop

/**
  * Generates a histogram from a column of a DataFrame
  *
  * @param df       Input DataFrame
  * @param column   The column to be used to generate the histogram
  * @param strategy The strategy to be used to generate the histogram.
  */
class HistogramGenerator(df: DataFrame, column: String, strategy: BucketStrategy.Value = BucketStrategy.STURGES) {
  private var customBins: Option[Int] = None

  def this(df: DataFrame, column: String, strategy: BucketStrategy.Value = BucketStrategy.STURGES,numBins: Int) {
    this(df, column, BucketStrategy.CUSTOM)
    customBins = Some(numBins)
  }

  /**
    * Calculates two arrays: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    *
    * @return The two arrays containing the histogram information.
    */
  def generate(): (Array[Double], Array[Long]) = {
    import BucketStrategy._
    val bins = strategy match {
      case SCOTT => getScottBins()
      case FRIEDMAN_DIACONIS => freedmanDiaconisBins()
      case SQUARE_ROOT => squareRootBins()
      case RICE_RULE => riceRuleBins()
      case STURGES => sturgesBins()
      case CUSTOM => if (customBins.isDefined) customBins.get else throw new IllegalArgumentException("Custom strategy selected but numBins not set.")
      case _ => throw new IllegalArgumentException("Invalid bin strategy")
    }
    histogramGivenBins(bins.toInt)
  }

  /**
    *
    * Generates a histogram and returns a dataframe with two columns: The bucket range on a string and the count of each bucket.
    * @return The DataFrame containing the histogram data
    */
  def generateDF():DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val (buckets,counts) = generate()
    //Calculate bucket ranges
    val bucketStrings = buckets.map(_.toString)
    val bucketRanges = bucketStrings.zipAll(bucketStrings.tail,"","").map{case(lowerBound,upperBound) => s"[$lowerBound,$upperBound["}

    //Create DF with bucket range strings
    bucketRanges.zip(counts).toSeq.toDF("bucket","count").orderBy("bucket")
  }


  private def squareRootBins(): Double = {
    Math.sqrt(df.count())
  }

  private def freedmanDiaconisBins(): Double = {
    import org.apache.commons.math3.stat.descriptive._

    val columnArray = df.select(column).rdd.map(row => row(0).asInstanceOf[Double]).collect()
    val arrMean = new DescriptiveStatistics()
    genericArrayOps(columnArray).foreach(v => arrMean.addValue(v))

    val q1 = arrMean.getPercentile(25)
    val q3 = arrMean.getPercentile(75)
    val iqr = q3 - q1

    2 * iqr * Math.pow(df.count(), -1 / 3)
  }

  private def riceRuleBins(): Double = {
    Math.ceil(2 * Math.pow(df.count(), 1 / 3))
  }

  private def sturgesBins(): Double = {
    (3.322 * Math.ceil(Math.log(df.count()) / Math.log(2))) + 1
  }

  private def getScottBins(): Double = {
    val deviation = df.select(stddev_pop(df(column))).collect().last.getDouble(0)
    (3.5 * deviation) / Math.pow(df.count(), 1 / 3)
  }

  /**
    * Calculates two arrays: the first one contains the starting values of each bin, the second one
    * contains the count for each bin
    *
    * @param bins The specific number of bins to be used to generate the histogram.
    * @return The two arrays containing the histogram information.
    */
  private def histogramGivenBins(bins: Int) = {
    import df.sparkSession.implicits._
    df.select(column).map(value => value.getDouble(0)).rdd.histogram(bins)
  }
}

object BucketStrategy extends Enumeration {
  val SCOTT,FRIEDMAN_DIACONIS,SQUARE_ROOT,RICE_RULE,STURGES,CUSTOM = Value
}