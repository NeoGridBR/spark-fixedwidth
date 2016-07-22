package com.quartethealth.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.types.{StructField, StructType, _}

/**
  * Created by aichida on 01/06/2016.
  */
object MetadataFields {

  private val PositionKey = "width"
  private val DecimalDigitsKey = "decimalDigits"

  /**
    * Insert decimal separator in raw value of token.
    * Example: 001052 to 0010.52 with 2 decimal digits
    **/
  private def formatDecimalDigits(token: String, decimalDigits: Int): String = {
    if(token == null || token == "") return token;
    val (integer, decimal) = token.splitAt(token.length - decimalDigits)
    integer + '.' + decimal
  }

  /**
    * Format token applying the metadata properties
    *
    * @param field - Field that contain metadata
    * @param token - Token to formatting based on metadata
    * @return formatedToken based on metadata
    *
    **/
  def formatTokenBasedOnMetadata(token: String, field: StructField): String = {
    field.dataType match {
      case FloatType | DoubleType => {
        if (field.metadata.contains(DecimalDigitsKey))
          formatDecimalDigits(token, field.metadata.getLong(DecimalDigitsKey).toInt)
        else token
      }
      case _ => token
    }
  }

  /**
    * Retrieve the fixedwidth positions from metadata defined on schema
    *
    * @param schema - StructType of dataframe
    * @return - Array with positions
    **/
  def getFixedWidthPositionsFromMetadata(schema: StructType): Array[Int] = {
    val fixedWidths = new Array[Int](schema.length)
    for (i <- 0 to (schema.length) - 1) {
      val field = schema.fields(i)
      validateFieldPosition(field)
      fixedWidths(i) = field.metadata.getLong(PositionKey).toInt
    }
    fixedWidths
  }

  /**
    * Validate if position is correctly defined in schema metadata.
    *
    * @param field - Field to be validated
    * @throws - IllegalArgumentException if the field have a invalid position
    **/
  private def validateFieldPosition(field: StructField): Unit = {
    if (!field.metadata.contains(PositionKey)) {
      throw new SparkException(s"Field ${field.name} must have a fixediwidth position defined in the field metadata")
    }
  }

}
