package com.bnp.str.purge.utility

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter, Reader, Writer}
import java.nio.charset.StandardCharsets

object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  // -------------------------------------------------------------------------------------------
  // config access — the same optional-key idiom used by the other modules, gathered here so a
  // missing key reads the same way everywhere
  // -------------------------------------------------------------------------------------------

  def getStringOr(cfg: Config, key: String, default: String): String =
    if (cfg.hasPath(key)) cfg.getString(key) else default

  def getIntOr(cfg: Config, key: String, default: Int): Int =
    if (cfg.hasPath(key)) cfg.getInt(key) else default

  def getBooleanOr(cfg: Config, key: String, default: Boolean): Boolean =
    if (cfg.hasPath(key)) cfg.getBoolean(key) else default

  def getStringList(cfg: Config, key: String): Seq[String] = {
    import scala.collection.JavaConverters._
    if (cfg.hasPath(key)) cfg.getStringList(key).asScala.map(_.trim).filter(_.nonEmpty)
    else Seq.empty[String]
  }

  // -------------------------------------------------------------------------------------------
  // path handling
  //
  // Every path comparison in the engine goes through here. Two paths that denote the same object
  // may be written differently (`hdfs://nn/data/x`, `/data/x/`, `/data//x`), and a containment
  // check that misses one of those spellings is exactly how a purge escapes its allowed root.
  // -------------------------------------------------------------------------------------------

  /**
   * Canonical form used for every comparison: scheme and authority dropped, duplicate separators
   * collapsed, trailing separator removed. `hdfs://nn/data/rwa/` and `/data//rwa` both give
   * `/data/rwa`.
   *
   * Dropping the authority is deliberate and safe HERE because the engine works against one
   * filesystem per run (`fs.defaultFS`); it must not be used to compare paths across clusters.
   */
  def normalizePath(path: String): String = {
    val raw = Option(path).map(_.trim).getOrElse("")
    if (raw.isEmpty) ""
    else {
      val withoutScheme =
        try Option(new Path(raw).toUri.getPath).getOrElse(raw)
        catch { case _: Throwable => raw }
      val collapsed = withoutScheme.replaceAll("/+", "/")
      if (collapsed.length > 1 && collapsed.endsWith("/")) collapsed.dropRight(1) else collapsed
    }
  }

  /**
   * Canonical form of a path the operator CONFIGURED, resolved the way the filesystem will resolve
   * it — a relative `localRun/purge/input` becomes the absolute path it actually denotes.
   *
   * This exists because the inventory reports what the FileSystem returns, which is always
   * absolute, while a conf may name the same object relatively. Comparing the two forms silently
   * matches nothing: a policy would apply to no object, a scope would select no object, and an
   * allowed root would contain no object — each failing in the safe direction, and each looking
   * exactly like "there was nothing to purge". Every configured path therefore goes through here
   * before it is compared with an inventoried one.
   */
  def qualifyPath(path: String)(implicit sparkSession: SparkSession): String = {
    val raw = Option(path).map(_.trim).getOrElse("")
    if (raw.isEmpty) ""
    else
      try {
        val p = new Path(raw)
        normalizePath(p.getFileSystem(sparkSession.sparkContext.hadoopConfiguration).makeQualified(p).toString)
      } catch {
        case _: Throwable => normalizePath(raw)
      }
  }

  /**
   * The same resolution for a GLOB: only the literal prefix is resolved, the wildcard tail is
   * re-attached untouched. Resolving the whole pattern would ask the filesystem to interpret the
   * wildcard, which is not what a pattern means here.
   */
  def qualifyPattern(pattern: String)(implicit sparkSession: SparkSession): String = {
    val raw = Option(pattern).map(_.trim).getOrElse("")
    if (raw.isEmpty) ""
    else {
      val prefix = literalPrefixOf(raw)
      val suffix = raw.substring(prefix.length)
      val qualifiedPrefix = qualifyPath(prefix)
      // qualifyPath drops the trailing separator the pattern needs before its wildcard
      if (suffix.nonEmpty && prefix.endsWith("/")) qualifiedPrefix + "/" + suffix else qualifiedPrefix + suffix
    }
  }

  /** Number of non-empty segments: `/data/promethee/str/rwa` = 4, `/` = 0. */
  def pathDepth(path: String): Int =
    normalizePath(path).split("/").count(_.nonEmpty)

  /** Last segment of a path — the file or partition folder name. */
  def baseName(path: String): String = {
    val n = normalizePath(path)
    val cut = n.lastIndexOf('/')
    if (cut >= 0) n.substring(cut + 1) else n
  }

  /** Parent of a path, or "" for a root-level path. */
  def parentPath(path: String): String = {
    val n = normalizePath(path)
    val cut = n.lastIndexOf('/')
    if (cut > 0) n.substring(0, cut) else if (cut == 0) "/" else ""
  }

  /**
   * True when `path` IS `root` or lies strictly inside it. The separator check is what makes
   * `/data/rwa-archive` fail against root `/data/rwa` — a plain `startsWith` would let it through.
   */
  def isUnderRoot(path: String, root: String): Boolean = {
    val p = normalizePath(path)
    val r = normalizePath(root)
    r.nonEmpty && p.nonEmpty && (p == r || p.startsWith(if (r.endsWith("/")) r else r + "/"))
  }

  def isUnderAnyRoot(path: String, roots: Seq[String]): Boolean =
    roots.exists(isUnderRoot(path, _))

  /**
   * The reason `path` may never be touched, or None when it is structurally acceptable.
   *
   * This is the path half of the guard described in the specification (PC03), kept as a pure
   * function so it can be called from the reader (to refuse a scan root), from the controls, and
   * from `PurgeGuard` — three places that must agree.
   */
  def unsafePathReason(path: String, allowedRoots: Seq[String]): Option[String] = {
    val p = normalizePath(path)
    if (p.isEmpty) Some("path is empty")
    else if (path.contains("..")) Some(s"path '$path' contains '..'")
    else if (path.contains("*") || path.contains("?")) Some(s"path '$path' still contains a wildcard")
    else if (PrimaryConstants.FORBIDDEN_PATHS.contains(p)) Some(s"path '$p' is on the forbidden list")
    else if (pathDepth(p) < PrimaryConstants.MIN_PATH_DEPTH)
      Some(s"path '$p' has depth ${pathDepth(p)}, below the minimum ${PrimaryConstants.MIN_PATH_DEPTH}")
    else if (allowedRoots.nonEmpty && !isUnderAnyRoot(p, allowedRoots))
      Some(s"path '$p' is outside the allowed roots [${allowedRoots.mkString(", ")}]")
    else None
  }

  /**
   * Value of a Hive-style partition column found anywhere in a path:
   * `extractPartitionValue("/data/t/as_of_date=2023-03-31/part-0", "as_of_date")` = `2023-03-31`.
   * The LAST occurrence wins, so a nested layout resolves to the innermost value.
   *
   * The KEY is matched case-insensitively; the value is returned exactly as written. The projection
   * engine writes `runId=` on disk while the metastore stores `runid`, and the two spellings reach
   * this function from different directions. A case-sensitive match would simply not recognise a
   * partition as a run — no error, no row in the catalogue, and a purge that quietly has nothing to
   * do. The value keeps its case because it is a UUID, a date, a scenario name: identity, not a key.
   */
  def extractPartitionValue(path: String, column: String): Option[String] = {
    val col = Option(column).map(_.trim).getOrElse("")
    if (col.isEmpty) None
    else {
      val prefix = (col + "=").toLowerCase
      normalizePath(path).split("/")
        .filter(_.toLowerCase.startsWith(prefix))
        .lastOption
        .map(_.substring(col.length + 1))
        .filter(_.nonEmpty)
    }
  }

  /** Every `k=v` segment of a path, joined back as a Hive partition spec: `a=1/b=2`. Empty when none. */
  def partitionSpecOf(path: String): String =
    normalizePath(path).split("/").filter(s => s.contains("=") && !s.startsWith("=")).mkString("/")

  /**
   * A partition spec with its KEYS lowercased and its values untouched: `runId=9df8` -> `runid=9df8`.
   *
   * Hive lowercases partition column names in the metastore while the engine writes the directory
   * in its own spelling — the projection engine writes `runId=` with a capital I. Comparing the two
   * spellings as written finds nothing, which is the failure that looks like success: a purge that
   * matched no partition and a purge with nothing to do are the same empty result.
   */
  def normalizePartitionSpec(spec: String): String =
    Option(spec).getOrElse("").split("/").filter(_.nonEmpty).map { segment =>
      val cut = segment.indexOf('=')
      if (cut <= 0) segment else segment.substring(0, cut).toLowerCase + segment.substring(cut)
    }.mkString("/")

  // -------------------------------------------------------------------------------------------
  // glob patterns
  //
  // `scan.tables` and a policy's `pathPattern` / `tablePattern` are globs, never regexes: an
  // operator writing `/data/promethee/str/rwa/*` must not have to think about what `.` means, and
  // a table called `ts.ead` must match itself rather than silently widening the scope.
  // -------------------------------------------------------------------------------------------

  /**
   * Anchored regex for a glob where only `*` (any run of characters) and `?` (one character) are
   * wildcards; every other character is quoted. Case-insensitive, because Hive is.
   */
  def globToRegexString(pattern: String): String = {
    val translated = Option(pattern).getOrElse("").map {
      case '*' => ".*"
      case '?' => "."
      case c => java.util.regex.Pattern.quote(c.toString)
    }.mkString
    "(?i)^" + translated + "$"
  }

  /**
   * The literal part of a glob before its first wildcard: the pattern `/data/promethee/str/rwa/`
   * followed by a `*` has the prefix `/data/promethee/str/rwa/`.
   *
   * This is what makes "longest matching pattern wins" a defined rule rather than a feeling: when
   * two policies both match a path, the one whose literal prefix is longer is the more specific,
   * so a per-table policy always beats the per-domain catch-all above it.
   */
  def literalPrefixOf(pattern: String): String = {
    val p = Option(pattern).getOrElse("")
    val cut = Seq(p.indexOf('*'), p.indexOf('?')).filter(_ >= 0)
    if (cut.isEmpty) p else p.substring(0, cut.min)
  }

  /** Byte count for a human: `1536` -> `1.5 KiB`. Reports are read by people, not by parsers. */
  def humanBytes(bytes: Long): String = {
    val unit = 1024L
    if (bytes < unit) s"$bytes B"
    else {
      val units = Array("KiB", "MiB", "GiB", "TiB", "PiB", "EiB")
      var value = bytes.toDouble / unit
      var idx = 0
      while (value >= unit && idx < units.length - 1) { value /= unit; idx += 1 }
      f"$value%.1f ${units(idx)}"
    }
  }

  // -------------------------------------------------------------------------------------------
  // IO — read the conf, write a report, write a DataFrame. All through Hadoop's FileSystem, so
  // the same call takes a local path (`localRun/…`) or an HDFS one (`hdfs:///user/…`).
  // -------------------------------------------------------------------------------------------

  /**
   * Read a configuration file through Hadoop's FileSystem, decoded as UTF-8.
   *
   * The charset is explicit rather than the platform default: these files are written on Linux and
   * read wherever the job runs, and a French comment or an accented run name decoded as CP-1252
   * would corrupt values silently.
   */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
  }

  def getHdfsWriter(filePath: String)(sc: SparkContext): Writer = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    Option(path.getParent).foreach(parent => fs.mkdirs(parent))
    new BufferedWriter(new OutputStreamWriter(fs.create(path, true), StandardCharsets.UTF_8))
  }

  /**
   * Write a whole string (an HTML control report) to `filePath`, overwriting it.
   *
   * The checksum sidecar the local filesystem writes alongside is removed: the reports directory is
   * browsed by people looking for the document to read before approving a purge, and a hidden
   * `.report.html.crc` beside it is one more thing to explain. HDFS writes no sidecar, so this is a
   * no-op on the cluster.
   */
  def writeStringToHdfs(filePath: String, content: String)(sc: SparkContext): Unit = {
    val w = getHdfsWriter(filePath)(sc)
    try w.write(content) finally w.close()

    val target = new Path(filePath)
    val fs = target.getFileSystem(sc.hadoopConfiguration)
    val checksum = new Path(target.getParent, s".${target.getName}.crc")
    if (fs.exists(checksum)) fs.delete(checksum, false)
  }

  /**
   * Write one of the engine's outputs from its `purge_app.<tableName>` config block.
   *
   * Keys: `format`, `mode`, `numPartition`, `path`, `tableName`, and optionally `partitionBy`,
   * `delimiter` / `header` (text formats only). The output lands in `<path>/<tableName>`, the same
   * `<path>/<name>` convention the other engines use, so TWIST finds every module's output the
   * same way.
   */
  def writeDataframe(dataframe: DataFrame, tableName: String)(implicit sparkSession: SparkSession, conf: Config): Unit = {

    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.$tableName")
    val format = getStringOr(outConfig, "format", "orc")
    val mode = getStringOr(outConfig, "mode", PrimaryConstants.MODE_OVERWRITE)
    val numPartition = getIntOr(outConfig, "numPartition", 1)
    val path = outConfig.getString("path")
    val outTableName = getStringOr(outConfig, "tableName", tableName)
    val partitionBy = getStringList(outConfig, "partitionBy")
    val delimiter = getStringOr(outConfig, "delimiter", ";")
    val header = getStringOr(outConfig, "header", "true")

    val target = s"${normalizePath(path)}/$outTableName"
    log.info(s"Writing $tableName ($format, mode=$mode, numPartition=$numPartition" +
      (if (partitionBy.nonEmpty) s", partitionBy=${partitionBy.mkString(",")}" else "") +
      s") to $target")

    val writer = dataframe
      .coalesce(numPartition)
      .write
      .format(format)
      .option("header", header)
      .option("delimiter", delimiter)
      .option("emptyValue", "")
      .mode(mode)

    if (partitionBy.nonEmpty) writer.partitionBy(partitionBy: _*).save(target)
    else writer.save(target)
  }
}
