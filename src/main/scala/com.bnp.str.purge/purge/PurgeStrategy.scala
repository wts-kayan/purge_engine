package com.bnp.str.purge.purge

import com.bnp.str.purge.utility.PrimaryConstants

/**
 * How one object is removed.
 *
 * The strategy comes from the policy that governs the object, or from `execution.defaultStrategy`
 * when no policy does — which, since the business decided not to maintain a retention referential
 * (open question Q3), is the normal case.
 */
sealed trait PurgeStrategy {
  def name: String

  /** True when the object can be brought back afterwards. */
  def reversible: Boolean
}

object PurgeStrategy {

  /**
   * Move the data to HDFS Trash. Recoverable for `fs.trash.interval`, which on Promethee is
   * **7 days** (Q5). The default, and the reason a purge can be called reversible at all.
   */
  case object Trash extends PurgeStrategy {
    val name = PrimaryConstants.STRATEGY_TRASH
    val reversible = true
  }

  /** Delete outright. Nothing comes back. Only for a policy that demands it AND has an archive. */
  case object Hard extends PurgeStrategy {
    val name = PrimaryConstants.STRATEGY_HARD
    val reversible = false
  }

  /**
   * Drop the Hive partition.
   *
   * Never used alone. Every STR engine writes EXTERNAL tables with `external.table.purge = TRUE`
   * (Q8), so a bare `DROP PARTITION` deletes the data itself — through Hive, outside this engine's
   * deletion path, and with no certainty that it honours Trash. The executor therefore always moves
   * the data first and drops the partition second: by the time Hive is asked, the location is
   * already empty, so the drop is pure metadata and the 7-day window still applies.
   */
  case object DropPartition extends PurgeStrategy {
    val name = PrimaryConstants.STRATEGY_DROP_PARTITION
    val reversible = true
  }

  /** Record the object as purged without removing it — for a reader that cannot take a gap. */
  case object Logical extends PurgeStrategy {
    val name = PrimaryConstants.STRATEGY_LOGICAL
    val reversible = true
  }

  val All: Seq[PurgeStrategy] = Seq(Trash, Hard, DropPartition, Logical)

  /**
   * Resolve the strategy named on a manifest row, falling back to the run's default.
   *
   * An unrecognised name is an error, never a fallback: silently treating an unknown strategy as
   * TRASH would be merciful and silently treating it as HARD would be a disaster, so neither is
   * guessed. `PolicyReader` already refuses an unknown strategy at read time; this is the second
   * gate, for a manifest that reached execution some other way.
   */
  def of(name: String, default: PurgeStrategy): PurgeStrategy = {
    val wanted = Option(name).map(_.trim.toUpperCase).getOrElse("")
    if (wanted.isEmpty) default
    else All.find(_.name == wanted).getOrElse(throw new IllegalArgumentException(
      s"Unknown purge strategy '$name'. Expected one of: ${All.map(_.name).mkString(", ")}"))
  }
}
