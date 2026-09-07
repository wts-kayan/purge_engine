package com.bnp.str.purge.mapping

import com.bnp.str.purge.common.MapperProvider
import com.bnp.str.purge.utility.{DateUtils, PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.Date
import java.time.LocalDate

/**
 * Builds the purge candidate set: registers the five inputs as temp views, registers the two date
 * UDFs the selection SQL needs, and runs either the built-in
 * [[PrimaryView.get_purge_candidates]] or, when `queryName` is set on the output config, a query
 * loaded from the SQL queries file.
 *
 * Same idiom as `com.bnp.str.addons.mapping.PrimaryMapper`: the mapper owns the views and the
 * query choice, the view object owns the SQL.
 */
class PrimaryMapper(hdfs_inventory: DataFrame,
                    hive_catalog: DataFrame,
                    purge_policy: DataFrame,
                    purge_scope: DataFrame,
                    outputTableName: String,
                    today: LocalDate)
                   (implicit sparkSession: SparkSession, config: Config) extends MapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def getDataFrame_purge: DataFrame = {
    registerDateFunctions()

    // Cheap: the scope is built on the driver from the conf and the engine run configurations, so
    // this is a look at a handful of rows, not a scan.
    val scopeIsExclusive = !purge_scope.head(1).isEmpty

    hdfs_inventory.createOrReplaceTempView(PrimaryConstants.VIEW_HDFS_INVENTORY)
    hive_catalog.createOrReplaceTempView(PrimaryConstants.VIEW_HIVE_CATALOG)
    purge_policy.createOrReplaceTempView(PrimaryConstants.VIEW_PURGE_POLICY)
    purge_scope.createOrReplaceTempView(PrimaryConstants.VIEW_PURGE_SCOPE)

    val outConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$outputTableName")
    val queryName = PrimaryUtilities.getStringOr(outConfig, "queryName", "")

    val sql =
      if (queryName.nonEmpty) PrimaryView.loadQuery(queryName)(sparkSession, config)
      else PrimaryView.get_purge_candidates(today, scopeIsExclusive)

    log.info(s"Selecting purge candidates as of $today, " +
      (if (scopeIsExclusive) "restricted to what was explicitly selected" else "driven by the retention policy") +
      (if (queryName.nonEmpty) s", with the custom query '$queryName'" else ", with the built-in query"))

    sparkSession.sql(sql)
  }

  /**
   * The two date functions the selection SQL calls, both thin wrappers over the tested pure
   * functions in [[DateUtils]] — the retention arithmetic has exactly one implementation, and it is
   * the one covered by `DateUtilsSpec`.
   *
   * Both return null rather than throwing on bad input: an unparseable partition value simply falls
   * back to the file mtime, and an unusable retention unit cannot reach here at all because
   * `PolicyReader` refuses the referential outright. A UDF that throws would surface as an opaque
   * task failure instead of the clear message the reader already produces.
   */
  private def registerDateFunctions(): Unit = {
    val parseBusinessDate = udf { raw: String =>
      DateUtils.parseBusinessDate(raw).map(Date.valueOf).orNull
    }

    val retentionCutoff = udf { (reference: Date, value: Integer, unit: String) =>
      if (reference == null || value == null || unit == null) null
      else
        try Date.valueOf(DateUtils.cutoffDate(reference.toLocalDate, value.intValue(), unit))
        catch { case _: IllegalArgumentException => null }
    }

    sparkSession.udf.register(PrimaryView.UDF_BUSINESS_DATE, parseBusinessDate)
    sparkSession.udf.register(PrimaryView.UDF_RETENTION_CUTOFF, retentionCutoff)
  }
}
