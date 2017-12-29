package com.singularities.spark.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Various utilities dealing with DataFrames
  */
object DataFrameUtils {

  /**
    * Writes a DataFarme into a single CSV file
    *
    * @param df     The dataframe to be written
    * @param withHeader Boolean indicating if the dataframe has a header
    * @param sep separator of fields in the CSV file
    * @param path   Path indicating where the file will be written
    */
  def writeCSV(df: DataFrame, path: String, withHeader: Boolean = true, sep: String = ",", saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    import java.io.File
    import java.nio.file.{Files, Paths}

    //Coalesce and write with spark format (.csv folder with one .csv file with obscure name)
    df.repartition(1).write.options(Map("header" -> withHeader.toString, "sep" -> sep)).csv(path)
    val csvFolderPath = new File(path).toPath

    //Read the csv inside the folder, and move it inside the parent of the folder
    val csvFilePath = Files.newDirectoryStream(csvFolderPath, "*.csv").iterator.next
    val parentPath = csvFilePath.toFile.getParentFile.getParentFile.toPath
    val newCsvFilePath = Paths.get(parentPath.toString + "/" + csvFilePath.getFileName)
    Files.move(csvFilePath, newCsvFilePath)

    //Delete the .csv folder and contents.
    csvFolderPath.toFile.listFiles.foreach(f => Files.delete(f.toPath))
    Files.delete(csvFolderPath)

    //Rename .csv file to  '../parent/file.csv'
    val finalCsvPath = Paths.get(newCsvFilePath.toFile.getParentFile + "/" + csvFolderPath.getFileName)
    Files.move(newCsvFilePath, finalCsvPath)
  }
}
