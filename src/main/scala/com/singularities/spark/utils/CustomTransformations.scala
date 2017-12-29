package com.singularities.spark.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._


object CustomTransformations {

  /**
    * Changes the column type to another given as argument
    * @param cols Columns to be changed
    * @param t  The new datatype
    * @param tr Input dataframe
    * @return The dataframe with the column types changed
    */
  def convertToType(cols: Seq[String])(t:org.apache.spark.sql.types.DataType)(tr: DataFrame): DataFrame = {
    val trConverted = cols.foldLeft(tr)((df,colName) =>{
      val tmp = colName+"_x"
      df.withColumn(tmp, df.col(colName).cast(t))
        .drop(colName)
        .withColumnRenamed(tmp, colName);
    })
    trConverted
  }

  // No creo que este y convertToInt sean necesarios si está el convertToType, pero igual lo incluí
  def convertToDouble(cols: Seq[String])(tr: DataFrame): DataFrame = {
    import org.apache.spark.sql.types._
    val trConverted = cols.foldLeft(tr)((df,colName) => {
      val tmp = colName+"_x"
      df.withColumn(tmp, df.col(colName).cast(DoubleType))
        .drop(colName)
        .withColumnRenamed(tmp, colName);
    })
    trConverted
  }

  def convertToInt(cols: Seq[String])(tr: DataFrame): DataFrame = {
    import org.apache.spark.sql.types._
    val trConverted = cols.foldLeft(tr)((df,colName) => {
      val tmp = colName+"_x"
      df.withColumn(tmp, df.col(colName).cast(IntegerType))
        .drop(colName)
        .withColumnRenamed(tmp, colName);
    })
    trConverted
  }


  /**
    * Converts all column names to lowercase and replaces white spaces with '_'
    * @param df Input dataframe
    * @return The dataframe with the column names changed
    */
  def columnStandardName(df: DataFrame): DataFrame = {
    var newDf = df.toDF(df.columns map(_.toLowerCase): _*)
    newDf = newDf.columns.foldLeft(df)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "_")))
    newDf
  }


  /**
    * Returns the transposed dataframe
    * @param transBy  The column that the dataframe will be transposed by
    * @param df The dataframe which will be transposed
    */
  def transpose(transBy: Seq[String])(df: DataFrame): DataFrame = {
    val (cols, types) = df.dtypes.filter{ case (c, _) => !transBy.contains(c)}.unzip
    require(types.distinct.length == 1)
    val kvs = explode(array(cols.map(c => struct(lit(c).alias("column_name"), col(c).alias("column_value"))): _*))
    val byExprs = transBy.map(col(_))
    df.select(byExprs:+kvs.alias("_kvs"):_*).select(byExprs ++ Seq(col("_kvs.column_name"),col("_kvs.column_value")):_*)
  }

  def plusMinusDays(col: String)(days: Int)(df: DataFrame) : DataFrame ={
    df.withColumn("new_"+col, date_add(df(col), days))
  }

  def plusMinusMonths(col: String)(months: Int)(df: DataFrame): DataFrame ={
    df.withColumn("new_"+col, add_months(df(col), months))
  }

  def replaceColumn(colName: String)(newCol: Column)(df: DataFrame): DataFrame ={
    df.withColumn(colName, newCol)
  }

}
