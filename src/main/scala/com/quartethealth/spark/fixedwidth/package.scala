package com.quartethealth.spark

import com.quartethealth.spark.csv.util.TextFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

package object fixedwidth {

  implicit class FixedwidthContext(sqlContext: SQLContext) extends Serializable {

    def fixedFile(filePath: String,
                  fixedWidths: Array[Int],
                  schema: StructType = null,
                  useHeader: Boolean = true,
                  mode: String = "PERMISSIVE",
                  comment: Character = null,
                  ignoreLeadingWhiteSpace: Boolean = true,
                  ignoreTrailingWhiteSpace: Boolean = true,
                  charset: String = TextFile.DEFAULT_CHARSET.name(),
                  inferSchema: Boolean = false,
                  treatEmptyValuesAsNulls: Boolean = false): DataFrame = {

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
        treatEmptyValuesAsNulls = treatEmptyValuesAsNulls)(sqlContext)

      sqlContext.baseRelationToDataFrame(fixedwidthRelation)
    }

    /**
      * Create a FixedwidthRelation with position value contained on schema metadata
      *
      * @see {FixedwidthContext.fixedFile}
      **/
    def fixedFileDefinedBySchema(filePath: String,
                                 schema: StructType = null,
                                 useHeader: Boolean = true,
                                 mode: String = "PERMISSIVE",
                                 comment: Character = null,
                                 ignoreLeadingWhiteSpace: Boolean = true,
                                 ignoreTrailingWhiteSpace: Boolean = true,
                                 charset: String = TextFile.DEFAULT_CHARSET.name(),
                                 inferSchema: Boolean = false,
                                 treatEmptyValuesAsNulls: Boolean = false): DataFrame = {

      val positionArray = MetadataFields.getFixedWidthPositionsFromMetadata(schema)
      fixedFile(filePath, positionArray, schema, useHeader, mode, comment, ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace, charset, inferSchema, treatEmptyValuesAsNulls)
    }
  }

}
