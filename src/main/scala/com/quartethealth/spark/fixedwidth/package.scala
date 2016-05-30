package com.quartethealth.spark

import com.databricks.spark.csv.util.TextFile
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

package object fixedwidth {

  implicit class FixedwidthContext(sqlContext: SQLContext) extends Serializable {

    def fixedFile(
                   filePath: String,
                   fixedWidths: Array[Int],
                   schema: StructType = null,
                   useHeader: Boolean = true,
                   mode: String = "PERMISSIVE",
                   comment: Character = null,
                   ignoreLeadingWhiteSpace: Boolean = true,
                   ignoreTrailingWhiteSpace: Boolean = true,
                   charset: String = TextFile.DEFAULT_CHARSET.name(),
                   inferSchema: Boolean = false): DataFrame = {

      val fixedwidthRelation = new FixedwidthRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        comment = comment,
        parseMode = mode,
        fixedWidths = fixedWidths,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        userSchema = schema,
        inferSchema = inferSchema,
        treatEmptyValuesAsNulls = false)(sqlContext)
      sqlContext.baseRelationToDataFrame(fixedwidthRelation)
    }

    /**
      * Create a FixedwidthRelation with position value contained on schema metadata
      *
      * @see {FixedwidthContext.fixedFile}
      **/
    def fixedFileDefinedBySchema(
                                  filePath: String,
                                  schema: StructType = null,
                                  useHeader: Boolean = true,
                                  mode: String = "PERMISSIVE",
                                  comment: Character = null,
                                  ignoreLeadingWhiteSpace: Boolean = true,
                                  ignoreTrailingWhiteSpace: Boolean = true,
                                  charset: String = TextFile.DEFAULT_CHARSET.name(),
                                  inferSchema: Boolean = false): DataFrame = {
      val PositionProperty = "position"

      /**
        * Validate if position is correctly defined in schema metadata.
        *
        * @param field - Field to be validated
        * @throws - IllegalArgumentException if the field have a invalid position
        **/
      def validateFieldPosition(field: StructField): Unit = {
        if (!field.metadata.contains(PositionProperty)) {
          throw new SparkException(s"Field ${field.name} must have a fixediwidth position defined in the field metadata")
        }
      }

      val fixedWidths = new Array[Int](schema.length)
      for (i <- 0 to (schema.length) - 1) {
        val field = schema.fields(i)
        validateFieldPosition(field)
        fixedWidths(i) = field.metadata.getLong(PositionProperty).toInt
      }

      fixedFile(filePath, fixedWidths, schema, useHeader, mode, comment, ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace, charset, inferSchema)
    }
  }

}
