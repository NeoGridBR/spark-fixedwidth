package com.quartethealth.spark.fixedwidth


import com.quartethealth.spark.csv.CsvRelation
import com.quartethealth.spark.fixedwidth.readers._
import com.quartethealth.spark.csv.readers._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

class FixedwidthRelation protected[spark] (
    baseRDD: () => RDD[String],
    fixedWidths: Array[Int],
    location: Option[String],
    useHeader: Boolean,
    parseMode: String,
    comment: Character,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType,
    inferSchema: Boolean,
    codec: String = null,
    nullValue: String = "")(@transient override val sqlContext: SQLContext)
  extends CsvRelation(
    baseRDD,
    location,
    useHeader,
    delimiter = '\0',
    quote = null,
    escape = null,
    comment = comment,
    parserLib = "UNIVOCITY",
    parseMode = parseMode,
    ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
    ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
    treatEmptyValuesAsNulls = treatEmptyValuesAsNulls,
    userSchema = userSchema,
    inferCsvSchema = true,
    codec = codec)(sqlContext) {

  protected override def getLineReader(): LineReader = {
    val commentChar: Char = if (comment == null) '\0' else comment
    new LineFixedwidthReader(fixedWidths, commentMarker = commentChar,
      ignoreLeadingSpace = ignoreLeadingWhiteSpace,
      ignoreTrailingSpace = ignoreTrailingWhiteSpace)
  }

  protected override def getBulkReader(header: Seq[String], iter: Iterator[String],
      split: Int): BulkReader = {
    val commentChar: Char = if (comment == null) '\0' else comment
    new BulkFixedwidthReader(iter, split, fixedWidths,
      headers = header, commentMarker = commentChar,
      ignoreLeadingSpace = ignoreLeadingWhiteSpace,
      ignoreTrailingSpace = ignoreTrailingWhiteSpace)
  }
}
