package com.quartethealth.spark.fixedwidth

import com.quartethealth.spark.MetadataFields
import org.apache.spark.sql.types._
import org.specs2.mutable.Specification

/**
  * Created by aichida on 01/06/2016.
  */
class MetadataFieldsSuite extends Specification {

  "MetadataFields" should {

    "Should insert decimal separator in float/double fields if it is specified in metadata" in {
      val field = StructField("aField", DoubleType, true, Metadata.fromJson("{\"decimalDigits\":2}"))
      val teste = MetadataFields.formatTokenBasedOnMetadata("0001052", field)
      teste mustEqual "00010.52"
    }

    "Should do nothing if decimaDigits is not specified in metadata" in {
      val token = "0001052"
      val field = StructField("aField", DoubleType, true, Metadata.empty)
      val teste = MetadataFields.formatTokenBasedOnMetadata(token, field)
      teste mustEqual token
    }

    "Should retrieve position array if position is defined in metadata" in {
      val schema = StructType(Seq(
        StructField("aField", DoubleType, true, Metadata.fromJson("{\"position\":2}")),
        StructField("bField", StringType, true, Metadata.fromJson("{\"position\":7}"))
      ))
      val positionArray = MetadataFields.getFixedWidthPositionsFromMetadata(schema)
      positionArray mustEqual Array[Int](2,7)
    }
  }


}
