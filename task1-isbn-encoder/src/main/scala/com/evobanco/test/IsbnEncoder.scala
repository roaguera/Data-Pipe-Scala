package com.evobanco.test.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {
    
    val ean = df.select(col("Name"),col("Year"),concat(lit("ISBN-EAN: "),substring(col("isbn"),7,3)).as("ISBN"))
    val group = df.select(col("Name"),col("Year"),concat(lit("ISBN-GROUP: "),substring(col("isbn"),11,2)).as("ISBN"))
    val publisher = df.select(col("Name"),col("Year"),concat(lit("ISBN-PUBLISHER: "),substring(col("isbn"),14,4)).as("ISBN"))
    val title = df.select(col("Name"),col("Year"),concat(lit("ISBN-TITLE: "),substring(col("isbn"),18,3)).as("ISBN"))
    
    val temp = df.unionAll(ean.select(col("Name"),col("Year"),col("ISBN")))
    val temp2 = temp.unionAll(group.select(col("Name"),col("Year"),col("ISBN")))
    val temp3 = temp2.unionAll(publisher.select(col("Name"),col("Year"),col("ISBN")))
    val output = temp3.unionAll(title.select(col("Name"),col("Year"),col("ISBN")))

    return output
  }
}

