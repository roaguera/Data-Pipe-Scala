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

      val checkvalid =
        df.select(col("Name"),col("Year"),col("ISBN"), when(col("isbn") === null , "NOVALID" ) 
          .when(length(col("isbn")) < 20 , "NOVALID" )
          .when(length(col("isbn")) > 20 , "NOVALID" )
          .when(col("isbn").isNull , "NOVALID" ) 
          .otherwise( "VALID" )as("STATUS"))

      val inputvalid = checkvalid.filter(col("STATUS") === "VALID")
          .select(col("Name"),col("Year"),col("ISBN"))

      val input = inputvalid.select(col("Name"),col("Year"),col("ISBN"))

            val ean = input.select(col("Name"),col("Year"),concat(lit("ISBN-EAN: "),substring(col("isbn"),7,3)).as("ISBN"))
            val group = input.select(col("Name"),col("Year"),concat(lit("ISBN-GROUP: "),substring(col("isbn"),11,2)).as("ISBN"))
            val publisher = input.select(col("Name"),col("Year"),concat(lit("ISBN-PUBLISHER: "),substring(col("isbn"),14,4)).as("ISBN"))
            val title = input.select(col("Name"),col("Year"),concat(lit("ISBN-TITLE: "),substring(col("isbn"),18,3)).as("ISBN"))
    
            val temp = input.unionAll(ean.select(col("Name"),col("Year"),col("ISBN")))
            val temp2 = temp.unionAll(group.select(col("Name"),col("Year"),col("ISBN")))
            val temp3 = temp2.unionAll(publisher.select(col("Name"),col("Year"),col("ISBN")))
            val output = temp3.unionAll(title.select(col("Name"),col("Year"),col("ISBN")))

    return output
  }
}

