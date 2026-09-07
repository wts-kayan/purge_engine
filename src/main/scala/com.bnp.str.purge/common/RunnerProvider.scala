package com.bnp.str.purge.common

import com.bnp.str.purge.reader.PrimaryReader
import com.bnp.str.purge.utility.PrimaryConstants
import org.apache.spark.sql.DataFrame

abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  private[common] lazy val hdfs_inventory: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.HDFS_INVENTORY)

  private[common] lazy val hive_catalog: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.HIVE_CATALOG)

  private[common] lazy val purge_policy: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PURGE_POLICY)

  private[common] lazy val purge_scope: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PURGE_SCOPE)

  private[common] lazy val run_history: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RUN_HISTORY)

  /** Inventory, select, control: everything a simulation does, up to but never including a write. */
  def run_purge_runner(runId: String): PurgeOutcome

}
