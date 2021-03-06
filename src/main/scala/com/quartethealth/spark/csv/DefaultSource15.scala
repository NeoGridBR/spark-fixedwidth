/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quartethealth.spark.csv


import org.apache.spark.sql.sources.DataSourceRegister

/* Extension of DefaultSource (which is Spark 1.3 and 1.4 compatible) for Spark 1.5 compatibility.
 * Since the class is loaded through META-INF/services we can decouple the two to have
 * Spark 1.5 byte-code loaded lazily.
 *
 * This trick is adapted from spark elasticsearch-hadoop data source:
 * <https://github.com/elastic/elasticsearch-hadoop>
 */
class DefaultSource15 extends DefaultSource with DataSourceRegister {

  /**
   * Short alias for spark-csv data source.
   */
  override def shortName(): String = "csv"
}
