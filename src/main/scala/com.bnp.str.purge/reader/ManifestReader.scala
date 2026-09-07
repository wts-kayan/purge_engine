package com.bnp.str.purge.reader

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Reads back the manifest a previous simulation wrote.
 *
 * This is what makes the two phases actually two phases. The execution does not recompute what to
 * delete, it replays a document — and reading that document back from ORC, rather than keeping it in
 * memory across a mode switch, is what makes the separation real: the thing executed is then
 * literally the thing that was written, looked at, and named by its run id.
 */
class ManifestReader()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * The manifest of `runId`, from `purge_manifest.path/purge_manifest`.
   *
   * Fails when the run is unknown. An execution asked to replay a manifest that is not there must
   * stop, not carry on with an empty one: an empty manifest deletes nothing, which sounds harmless
   * and reports a successful purge that never happened.
   */
  def read(runId: String): DataFrame = {
    require(Option(runId).exists(_.trim.nonEmpty),
      s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}.manifestRunId is empty: an " +
        "execution replays a named manifest, it does not build one")

    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.PURGE_MANIFEST}")
    val base = PrimaryUtilities.normalizePath(outConfig.getString("path"))
    val name = PrimaryUtilities.getStringOr(outConfig, "tableName", PrimaryConstants.PURGE_MANIFEST)
    val location = s"$base/$name"

    log.info(s"Replaying the manifest of run $runId from $location")

    val manifest = sparkSession.read.orc(location).where(col("run_id") === runId).cache()
    val rows = manifest.count()
    require(rows > 0L, s"no manifest found for run '$runId' under $location; re-run the simulation")

    log.info(s"Manifest $runId: $rows row(s)")
    manifest
  }
}
