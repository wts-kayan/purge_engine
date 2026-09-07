package com.bnp.str.purge.reader

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ContentSummary, FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import scala.collection.mutable

/**
 * Walks the scan roots on HDFS and emits one row per PURGE UNIT — the smallest thing a purge may
 * remove on its own.
 *
 * A purge unit is:
 *  - a LEAF_DIR: a directory with no sub-directory. This is the usual shape of a Hive partition
 *    folder (`as_of_date=2023-03-31/`), and it is the granularity the business reasons about —
 *    nobody purges half a partition. Its stats are the aggregate of the files it holds.
 *  - a FILE: a file sitting directly in a directory that ALSO has sub-directories. Rare in a
 *    partitioned layout, common at the top of a table folder (`_SUCCESS`, a stray CSV), and it must
 *    be listed rather than silently folded into a parent that is not itself a unit.
 *  - a DIR_TRUNCATED: a directory the walk stopped at because `scan.maxDepth` was reached. Its
 *    stats come from one NameNode `getContentSummary` call, so a scope is never reported as
 *    smaller than it is just because it is deeper than expected.
 *
 * WHY THE WALK IS SPLIT IN TWO. A single-threaded `listStatus` from the driver is how an inventory
 * job dies on a real datalake: the driver holds every FileStatus of a 100k-entry directory in one
 * array, and one slow NameNode call blocks the whole scan. Instead the driver descends only far
 * enough to produce `scan.parallelism` seed directories — emitting, as it goes, the units it
 * passes over so nothing is lost — and the executors walk the sub-trees below those seeds in
 * parallel, each with its own `listStatusIterator`, which streams instead of materialising.
 */
class InventoryReader()(implicit sparkSession: SparkSession, conf: Config) {

  import InventoryReader._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val scanConfig: Config = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}")

  // Resolved the way the filesystem resolves them, so a configured root and an inventoried path
  // are the same string when they are the same object — see PrimaryUtilities.qualifyPath.
  private val allowedRoots: Seq[String] =
    PrimaryUtilities.getStringList(scanConfig, PrimaryConstants.SCAN_ALLOWED_ROOTS)
      .map(PrimaryUtilities.qualifyPath)

  /**
   * What to walk: `scan.roots` when the configuration names them, and otherwise the locations of
   * the databases the engine writes to, asked of the metastore.
   *
   * The default is what makes an engine-driven purge need no path at all. A database's directory is
   * something the catalogue already knows; copying it into this file would be one more answer to
   * keep in step with the metastore, and one more thing to be quietly wrong the day a database moves.
   */
  private lazy val roots: Seq[String] = {
    val configured = PrimaryUtilities.getStringList(scanConfig, PrimaryConstants.SCAN_ROOTS)
      .map(PrimaryUtilities.qualifyPath)

    if (configured.nonEmpty) configured
    else {
      val fromEngine = new EngineRunReader().databaseRoots
      if (fromEngine.nonEmpty)
        log.info(s"scan.roots is empty; walking the engine's database(s) instead: " +
          fromEngine.mkString(", "))
      fromEngine
    }
  }

  private val maxDepth: Int =
    PrimaryUtilities.getIntOr(scanConfig, PrimaryConstants.SCAN_MAX_DEPTH, PrimaryConstants.DEFAULT_MAX_DEPTH)

  private val parallelism: Int =
    PrimaryUtilities.getIntOr(scanConfig, PrimaryConstants.SCAN_PARALLELISM, PrimaryConstants.DEFAULT_PARALLELISM)

  private val followSymlinks: Boolean =
    PrimaryUtilities.getBooleanOr(scanConfig, PrimaryConstants.SCAN_FOLLOW_SYMLINKS, default = false)

  /**
   * The inventory of the configured scope, as a DataFrame of [[InventoryReader.PurgeUnit]].
   *
   * Every root is validated BEFORE the first listing: it must be structurally safe and inside
   * `scan.allowedRoots`. This is the earliest point where a mistyped configuration can be caught,
   * and catching it here means no later phase ever sees a path it should not have seen.
   */
  def read(): DataFrame = {
    require(roots.nonEmpty,
      s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}.${PrimaryConstants.SCAN_ROOTS} is empty: " +
        "nothing to inventory")
    require(maxDepth > 0, s"scan.maxDepth must be > 0, got $maxDepth")

    validateRoots()

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val existingRoots = roots.filter { root =>
      val exists = new Path(root).getFileSystem(hadoopConf).exists(new Path(root))
      if (!exists) log.warn(s"Scan root does not exist, skipped: $root")
      exists
    }

    val seeded = seedFromDriver(existingRoots, hadoopConf)
    log.info(s"Inventory seeding done: ${seeded.units.size} unit(s) found while descending, " +
      s"${seeded.seeds.size} sub-tree(s) handed to the executors")

    import sparkSession.implicits._

    val confEntries = serializableHadoopConf(hadoopConf)
    val broadcastConf = sparkSession.sparkContext.broadcast(confEntries)
    val slices = math.max(1, math.min(parallelism, seeded.seeds.size))

    // Copied into locals BEFORE the closure: reading `maxDepth` / `followSymlinks` inside it would
    // capture `this`, and the reader holds a SparkSession and a typesafe Config, neither of which
    // is serializable. The failure is a NotSerializableException at the first executor-side walk —
    // invisible on a small scope that the driver seeding happens to cover on its own.
    val walkMaxDepth = maxDepth
    val walkFollowSymlinks = followSymlinks

    val fromExecutors =
      if (seeded.seeds.isEmpty) sparkSession.emptyDataset[PurgeUnit]
      else
        sparkSession.sparkContext
          .parallelize(seeded.seeds, slices)
          .mapPartitions { seeds =>
            val hconf = rebuildHadoopConf(broadcastConf.value)
            seeds.flatMap(seed => walk(seed, hconf, walkMaxDepth, walkFollowSymlinks).iterator)
          }
          .toDS()

    val fromDriver = sparkSession.createDataset(seeded.units)

    fromDriver.union(fromExecutors).toDF()
  }

  /**
   * Refuse a scan root that is unsafe or outside `allowedRoots`, naming every offender at once —
   * a configuration is fixed in one pass, not one error per run.
   */
  private def validateRoots(): Unit = {
    val offenders = roots.flatMap(root =>
      PrimaryUtilities.unsafePathReason(root, allowedRoots).map(reason => s"  - $root: $reason"))

    if (offenders.nonEmpty)
      throw new IllegalArgumentException(
        s"Refusing to scan; ${offenders.size} invalid root(s) in " +
          s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}.${PrimaryConstants.SCAN_ROOTS}:\n" +
          offenders.mkString("\n"))
  }

  /**
   * Descend from the roots until there are enough sub-trees to keep the executors busy, keeping
   * every unit met on the way. Stops early when the tree bottoms out — a shallow scope is walked
   * entirely on the driver, which is both correct and faster than shipping two tasks.
   */
  private def seedFromDriver(startRoots: Seq[String], hadoopConf: Configuration): Seeded = {
    val units = mutable.ArrayBuffer.empty[PurgeUnit]
    var frontier: Seq[Seed] = startRoots.map(root => Seed(PrimaryUtilities.normalizePath(root), 0, PrimaryUtilities.normalizePath(root)))

    var keepDescending = frontier.nonEmpty
    while (keepDescending && frontier.size < parallelism) {
      val next = mutable.ArrayBuffer.empty[Seed]
      var descendedSomewhere = false

      frontier.foreach { seed =>
        val (visitedUnits, subDirs) = visit(seed, hadoopConf, maxDepth, followSymlinks)
        units ++= visitedUnits
        if (subDirs.isEmpty) {
          // nothing below: this branch is fully accounted for by `visitedUnits`
        } else {
          descendedSomewhere = true
          next ++= subDirs
        }
      }

      frontier = next
      keepDescending = descendedSomewhere && frontier.nonEmpty
    }

    Seeded(units.toList, frontier.toList)
  }
}

object InventoryReader {

  /**
   * One row of the HDFS inventory. Field names are the output column names — snake_case, so the
   * selection SQL of P2 reads the same as the specification.
   *
   * @param access_time None when the cluster does not maintain atime (`dfs.namenode.accesstime.precision = 0`),
   *                    which is common; a rule reading it must treat the absence as "unknown", never as "old".
   */
  final case class PurgeUnit(path: String,
                             parent_path: String,
                             object_type: String,
                             root_path: String,
                             depth: Int,
                             size_bytes: Long,
                             num_files: Long,
                             modification_time: Timestamp,
                             access_time: Option[Timestamp],
                             owner_name: String,
                             group_name: String,
                             permission: String)

  /** A directory still to be walked, with its depth relative to the root it came from. */
  private[reader] final case class Seed(path: String, depth: Int, root: String)

  private[reader] final case class Seeded(units: Seq[PurgeUnit], seeds: Seq[Seed])

  // -----------------------------------------------------------------------------------------
  // The walk itself lives in the companion object and takes only serializable arguments: it runs
  // on the executors, where neither the SparkSession nor the (non-serializable) typesafe Config
  // exists.
  // -----------------------------------------------------------------------------------------

  private[reader] def serializableHadoopConf(conf: Configuration): Array[(String, String)] = {
    import scala.collection.JavaConverters._
    conf.iterator().asScala.map(e => (e.getKey, e.getValue)).toArray
  }

  private[reader] def rebuildHadoopConf(entries: Array[(String, String)]): Configuration = {
    val conf = new Configuration(false)
    entries.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * Walk one sub-tree to completion, iteratively. A stack rather than recursion: `scan.maxDepth`
   * bounds the depth today, but an iterative walk cannot be turned into a StackOverflowError by a
   * later change to that bound.
   */
  private[reader] def walk(seed: Seed,
                           hadoopConf: Configuration,
                           maxDepth: Int,
                           followSymlinks: Boolean): Seq[PurgeUnit] = {
    val out = mutable.ArrayBuffer.empty[PurgeUnit]
    val stack = mutable.Stack[Seed](seed)

    while (stack.nonEmpty) {
      val current = stack.pop()
      val (units, subDirs) = visit(current, hadoopConf, maxDepth, followSymlinks)
      out ++= units
      subDirs.foreach(stack.push)
    }

    out.toList
  }

  /**
   * Visit ONE directory and decide what it is.
   *
   * Returns the units it yields and the sub-directories still to visit, so the driver-side seeding
   * and the executor-side walk share exactly one definition of "what is a purge unit" — two
   * implementations would eventually disagree, and the disagreement would be invisible until a
   * purge missed something.
   */
  private[reader] def visit(seed: Seed,
                            hadoopConf: Configuration,
                            maxDepth: Int,
                            followSymlinks: Boolean): (Seq[PurgeUnit], Seq[Seed]) = {

    val path = new Path(seed.path)
    val fs = path.getFileSystem(hadoopConf)

    val self = try fs.getFileStatus(path) catch { case _: Throwable => null }
    if (self == null) return (Nil, Nil)                              // vanished between listing and visit
    if (self.isSymlink && !followSymlinks) return (Nil, Nil)
    if (!self.isDirectory) return (Seq(fileUnit(self, seed)), Nil)   // a root pointing straight at a file

    val children = listChildren(fs, path, followSymlinks)
    val (dirs, files) = children.partition(_.isDirectory)

    if (dirs.isEmpty) {
      // no sub-directory: this IS a purge unit, empty or not. An empty leaf directory is worth
      // reporting — it is exactly what a previous purge leaves behind.
      (Seq(leafDirUnit(self, files, seed)), Nil)
    } else if (seed.depth >= maxDepth) {
      // deeper than we were told to go: one NameNode summary rather than an unbounded descent
      (Seq(truncatedDirUnit(fs, self, seed)), Nil)
    } else {
      // files sitting alongside sub-directories are units in their own right
      (files.map(f => fileUnit(f, seed)), dirs.map(d => Seed(PrimaryUtilities.normalizePath(d.getPath.toString), seed.depth + 1, seed.root)))
    }
  }

  /** `listStatusIterator` streams from the NameNode instead of materialising a whole directory. */
  private def listChildren(fs: FileSystem, path: Path, followSymlinks: Boolean): Seq[FileStatus] = {
    val out = mutable.ArrayBuffer.empty[FileStatus]
    try {
      val it = fs.listStatusIterator(path)
      while (it.hasNext) {
        val status = it.next()
        if (followSymlinks || !status.isSymlink) out += status
      }
    } catch {
      case _: java.io.FileNotFoundException => // removed while we were walking; report nothing
    }
    out.toList
  }

  private def leafDirUnit(dir: FileStatus, files: Seq[FileStatus], seed: Seed): PurgeUnit = {
    val newest = (dir.getModificationTime +: files.map(_.getModificationTime)).max
    val lastAccess = (dir.getAccessTime +: files.map(_.getAccessTime)).max
    PurgeUnit(
      path = PrimaryUtilities.normalizePath(dir.getPath.toString),
      parent_path = PrimaryUtilities.parentPath(dir.getPath.toString),
      object_type = PrimaryConstants.OBJECT_TYPE_LEAF_DIR,
      root_path = seed.root,
      depth = seed.depth,
      size_bytes = files.map(_.getLen).sum,
      num_files = files.size.toLong,
      modification_time = new Timestamp(newest),
      access_time = optionalTimestamp(lastAccess),
      owner_name = safeMeta(dir.getOwner),
      group_name = safeMeta(dir.getGroup),
      permission = safeMeta(dir.getPermission.toString))
  }

  private def fileUnit(file: FileStatus, seed: Seed): PurgeUnit =
    PurgeUnit(
      path = PrimaryUtilities.normalizePath(file.getPath.toString),
      parent_path = PrimaryUtilities.parentPath(file.getPath.toString),
      object_type = PrimaryConstants.OBJECT_TYPE_FILE,
      root_path = seed.root,
      depth = seed.depth,
      size_bytes = file.getLen,
      num_files = 1L,
      modification_time = new Timestamp(file.getModificationTime),
      access_time = optionalTimestamp(file.getAccessTime),
      owner_name = safeMeta(file.getOwner),
      group_name = safeMeta(file.getGroup),
      permission = safeMeta(file.getPermission.toString))

  private def truncatedDirUnit(fs: FileSystem, dir: FileStatus, seed: Seed): PurgeUnit = {
    val summary: ContentSummary =
      try fs.getContentSummary(dir.getPath)
      catch { case _: Throwable => null }

    PurgeUnit(
      path = PrimaryUtilities.normalizePath(dir.getPath.toString),
      parent_path = PrimaryUtilities.parentPath(dir.getPath.toString),
      object_type = PrimaryConstants.OBJECT_TYPE_DIR_TRUNCATED,
      root_path = seed.root,
      depth = seed.depth,
      size_bytes = if (summary == null) 0L else summary.getLength,
      num_files = if (summary == null) 0L else summary.getFileCount,
      modification_time = new Timestamp(dir.getModificationTime),
      access_time = optionalTimestamp(dir.getAccessTime),
      owner_name = safeMeta(dir.getOwner),
      group_name = safeMeta(dir.getGroup),
      permission = safeMeta(dir.getPermission.toString))
  }

  /** HDFS reports 0 when atime is not maintained; that is "unknown", not "1 January 1970". */
  private def optionalTimestamp(epochMillis: Long): Option[Timestamp] =
    if (epochMillis <= 0L) None else Some(new Timestamp(epochMillis))

  /**
   * Owner, group and permission are read defensively.
   *
   * On the local filesystem these three fields are loaded lazily and go through Hadoop's native IO,
   * which is absent on a developer box without winutils — and on HDFS a `stat` can fail for a path
   * the job may still list. They are descriptive metadata: an inventory that cannot name the owner
   * of a directory is worth far more than no inventory at all, so a failure yields "" and the walk
   * carries on. A control that needs the owner must therefore treat "" as unknown.
   */
  private def safeMeta(read: => String): String =
    try Option(read).getOrElse("") catch { case _: Throwable => "" }
}
