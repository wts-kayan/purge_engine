package com.bnp.str.purge.writer

import com.bnp.str.purge.utility.PrimaryUtilities
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Writes an engine output from its `purge_app.<tableName>` config block.
 *
 * Same thin shape as the writer of the other modules: the writer owns the "which output" decision,
 * [[PrimaryUtilities.writeDataframe]] owns the format/mode/partitioning details, so a change to the
 * house write convention lands in one place for every module.
 */
class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  def write(dataframe: DataFrame,
            tableName: String)(sparkSession: SparkSession, conf: Config): Unit = {

    PrimaryUtilities.writeDataframe(dataframe, tableName)(sparkSession, conf)
  }

}
