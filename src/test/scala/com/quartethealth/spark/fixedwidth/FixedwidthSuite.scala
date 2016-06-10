package com.quartethealth.spark.fixedwidth

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkException}
import org.specs2.mutable.Specification
import org.specs2.specification.After

trait FixedwidthSetup extends After {
  protected def fruit_resource(name: String = ""): String =
    s"src/test/resources/fruit_${name}_fixedwidth.txt"

  protected val fruitWidths = Array(3, 10, 5, 4)
  protected val fruitSize = 7
  protected val malformedFruitSize = 5
  protected val fruitFirstRow = Seq(56, "apple", "TRUE", 0.56)

  protected val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", StringType),
    StructField("cost", DoubleType)
  ))
  protected val fruitSchemaWithMetadata = StructType(Seq(
    StructField("val", IntegerType, true, Metadata.fromJson("{\"width\":3}")),
    StructField("name", StringType, true, Metadata.fromJson("{\"width\":10}")),
    StructField("avail", StringType, true, Metadata.fromJson("{\"width\":5}")),
    StructField("cost", DoubleType, true, Metadata.fromJson("{\"width\":4}"))
  ))

  protected val fruitSchemaWithMetadataDecimalDigits = StructType(Seq(
    StructField("val", IntegerType, true, Metadata.fromJson("{\"width\":3}")),
    StructField("name", StringType, true, Metadata.fromJson("{\"width\":10}")),
    StructField("avail", StringType, true, Metadata.fromJson("{\"width\":5}")),
    StructField("cost", DoubleType, false, Metadata.fromJson("{\"width\":4, \"decimalDigits\":2}"))
  ))

  protected val fruitSchemaWithInvalidwidthMetadata = StructType(Seq(
    StructField("val", IntegerType, true, Metadata.fromJson("{\"width\":3}")),
    StructField("name", StringType, true, Metadata.empty),
    StructField("avail", StringType, true, Metadata.fromJson("{\"width\":5}")),
    StructField("cost", DoubleType, true, Metadata.fromJson("{\"width\":4}"))
  ))

  val sqlContext: SQLContext = new SQLContext(new SparkContext("local[2]", "FixedwidthSuite"))

  def after = sqlContext.sparkContext.stop()
}

class FixedwidthSpec extends Specification with FixedwidthSetup {

  protected def sanityChecks(resultSet: DataFrame) = {
    resultSet.show()
    resultSet.collect().length mustEqual fruitSize

    val head = resultSet.head()
    head.length mustEqual fruitWidths.length
    head.toSeq mustEqual fruitFirstRow
  }

  "FixedwidthParser" should {

    "Parse a basic fixed width file, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths, fruitSchema,
        useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with headers, and ignore them" in {
      val result = sqlContext.fixedFile(fruit_resource("w_headers"), fruitWidths,
        fruitSchema, useHeader = true)
      sanityChecks(result)
    }

    "Parse a fw file with overflowing lines, and ignore the overflow" in {
      val result = sqlContext.fixedFile(fruit_resource("overflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with underflowing lines, successfully " in {
      val result = sqlContext.fixedFile(fruit_resource("underflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema and without inferring types, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema, and infer the schema" in {
      val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = true)
      sanityChecks(result)
    }

    "Parse a fw file with headers but without schema and without inferrence, succesfully" in {
      val result = sqlContext.fixedFile(fruit_resource("w_headers"), fruitWidths,
        useHeader = true, inferSchema = true)
      sanityChecks(result)
    }

    "Parse a fw file with comments, and ignore those lines" in {
      val result = sqlContext.fixedFile(fruit_resource("comments"), fruitWidths,
        useHeader = true, inferSchema = true, comment = '/')
      sanityChecks(result)
    }

    "Parse a malformed fw and schemaless file in PERMISSIVE mode, successfully" in {
      val result = sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
        useHeader = false, mode = "PERMISSIVE")
      result.show()
      result.collect().length mustEqual fruitSize
    }

    "Parse a malformed and schemaless fw file in DROPMALFORMED mode, successfully dropping bad lines" in {
      val result = sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
        useHeader = false, mode = "DROPMALFORMED")
      result.show()
      result.collect().length mustEqual malformedFruitSize
    }

    "Parse with width defined in structfield metadata" in {
      val result = sqlContext.fixedFileDefinedBySchema(fruit_resource(), fruitSchemaWithMetadata,
        useHeader = false)
      sanityChecks(result)
    }

    "Parse with width defined in structfield metadata and include decimal digits" in {
      val result = sqlContext.fixedFileDefinedBySchema(fruit_resource("decimal"), fruitSchemaWithMetadataDecimalDigits,
        useHeader = false)
      sanityChecks(result)
    }

    "FAIL to parse a malformed fw file with schema in FAILFAST mode" in {
      def fail = {
        sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
          fruitSchema, useHeader = false, mode = "FAILFAST").collect()
      }
      fail must throwA[SparkException]
    }

    "FAIL to parse a fw file with the wrong format" in {
      def fail = {
        sqlContext.fixedFile(fruit_resource("wrong_schema"), fruitWidths,
          fruitSchema, useHeader = false).collect()
      }
      fail must throwA[SparkException]
    }

    "FAIL to parse a fw file with invalid width contained on schema" in {
      def fail = {
        sqlContext.fixedFileDefinedBySchema(fruit_resource(), fruitSchemaWithInvalidwidthMetadata, useHeader = false).collect()
      }
      fail must throwA[SparkException]
    }
  }

}
