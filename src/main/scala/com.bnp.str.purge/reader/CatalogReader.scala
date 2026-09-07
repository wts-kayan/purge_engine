package com.bnp.str.purge.reader

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * The metastore's view of the scan scope: one row per Hive table, and one row per registered
 * partition.
 *
 * The inventory ([[InventoryReader]]) sees files; this reader sees what Hive BELIEVES exists. The
 * purge needs both, for two different reasons:
 *
 *  - a path that is a registered partition must not be deleted on its own, or the metastore is
 *    left pointing at nothing (control PC11 — the executor drops the partition as well);
 *  - a partition registered on a path that no longer exists is already-orphaned metadata, which
 *    the report should show rather than hide.
 *
 * Everything here is best-effort PER TABLE. A single unreadable table — a broken SerDe, a
 * permission gap, a location on a filesystem this job cannot reach — is logged and skipped, never
 * allowed to abort the scan of a whole database. An inventory that stops at the first bad table is
 * an inventory nobody can run.
 */
class CatalogReader()(implicit sparkSession: SparkSession, conf: Config) {

  import CatalogReader._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val scanConfig: Config = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}")

  private val databases: Seq[String] =
    PrimaryUtilities.getStringList(scanConfig, PrimaryConstants.SCAN_DATABASES)

  /** Optional table filter; `*` and `?` are accepted. Empty means every table of the database. */
  private val tablePatterns: Seq[String] =
    PrimaryUtilities.getStringList(scanConfig, PrimaryConstants.SCAN_TABLES)

  /**
   * The catalog entries of the configured databases, as a DataFrame of
   * [[CatalogReader.CatalogEntry]]. Returns an empty DataFrame — not an error — when no database is
   * configured: a path-only purge scope is a legitimate way to run the engine.
   */
  def read(): DataFrame = {
    import sparkSession.implicits._

    if (databases.isEmpty) {
      log.info(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}.${PrimaryConstants.SCAN_DATABASES} " +
        "is empty; the catalog view is skipped (path-only scope)")
      return sparkSession.emptyDataset[CatalogEntry].toDF()
    }

    val entries = mutable.ArrayBuffer.empty[CatalogEntry]

    databases.foreach { database =>
      tablesOf(database).foreach { identifier =>
        try entries ++= entriesOf(identifier)
        catch {
          case e: Throwable =>
            log.warn(s"Catalog entry skipped for ${identifier.unquotedString}: " +
              s"${e.getClass.getSimpleName}: ${e.getMessage}")
        }
      }
    }

    log.info(s"Catalog read: ${entries.count(_.entry_kind == PrimaryConstants.CATALOG_TABLE)} table(s), " +
      s"${entries.count(_.entry_kind == PrimaryConstants.CATALOG_PARTITION)} partition(s) " +
      s"across ${databases.mkString(", ")}")

    sparkSession.createDataset(entries.toList).toDF()
  }

  /** Tables of one database, filtered by `scan.tables`. A missing database is a warning, not a stop. */
  private def tablesOf(database: String): Seq[TableIdentifier] =
    try {
      val catalog = sparkSession.sessionState.catalog
      if (!catalog.databaseExists(database)) {
        log.warn(s"Database '$database' does not exist in the metastore; skipped")
        Seq.empty
      } else {
        catalog.listTables(database).filter(id => matchesAnyPattern(id.table, tablePatterns))
      }
    } catch {
      case e: Throwable =>
        log.warn(s"Could not list the tables of '$database': ${e.getClass.getSimpleName}: ${e.getMessage}")
        Seq.empty
    }

  /** One row for the table, plus one row per registered partition when it is partitioned. */
  private def entriesOf(identifier: TableIdentifier): Seq[CatalogEntry] = {
    val catalog = sparkSession.sessionState.catalog
    val table: CatalogTable = catalog.getTableMetadata(identifier)

    val database = identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableLocation = table.storage.locationUri.map(uri => PrimaryUtilities.normalizePath(uri.toString)).getOrElse("")
    val partitionColumns = table.partitionColumnNames

    val tableEntry = CatalogEntry(
      database_name = database,
      table_name = identifier.table,
      entry_kind = PrimaryConstants.CATALOG_TABLE,
      table_type = table.tableType.name,
      provider = table.provider.getOrElse(""),
      partition_columns = partitionColumns.mkString(","),
      partition_spec = "",
      location_path = tableLocation,
      owner_name = table.owner,
      /* An EXTERNAL table whose data survives DROP TABLE is the shape every STR engine writes; the
         flag is carried through so a later control can refuse to drop a MANAGED partition without
         the operator having said so explicitly. */
      external_purge = table.properties.getOrElse("external.table.purge", ""))

    if (partitionColumns.isEmpty) Seq(tableEntry)
    else tableEntry +: partitionsOf(identifier, database, partitionColumns, tableEntry)
  }

  private def partitionsOf(identifier: TableIdentifier,
                           database: String,
                           partitionColumns: Seq[String],
                           tableEntry: CatalogEntry): Seq[CatalogEntry] =
    try {
      sparkSession.sessionState.catalog
        .listPartitions(identifier)
        .map { partition: CatalogTablePartition =>
          tableEntry.copy(
            entry_kind = PrimaryConstants.CATALOG_PARTITION,
            partition_spec = specOf(partition, partitionColumns),
            location_path = partition.storage.locationUri
              .map(uri => PrimaryUtilities.normalizePath(uri.toString))
              .getOrElse(""))
        }
    } catch {
      case e: Throwable =>
        log.warn(s"Could not list the partitions of ${identifier.unquotedString}: " +
          s"${e.getClass.getSimpleName}: ${e.getMessage}")
        Seq.empty
    }

  /**
   * `as_of_date=2023-03-31/scenario=FW` — in the table's own partition-column order, not the map
   * iteration order, so the spec can be compared with the path a directory actually has.
   */
  private def specOf(partition: CatalogTablePartition, partitionColumns: Seq[String]): String =
    PrimaryUtilities.normalizePartitionSpec(
      partitionColumns
        .flatMap(column => partition.spec.get(column).map(value => s"$column=$value"))
        .mkString("/"))
}

object CatalogReader {

  /**
   * One metastore entry. `entry_kind` distinguishes the table row from its partition rows; the
   * table row is kept even for a partitioned table, because a table with zero registered partitions
   * is itself a finding.
   */
  final case class CatalogEntry(database_name: String,
                                table_name: String,
                                entry_kind: String,
                                table_type: String,
                                provider: String,
                                partition_columns: String,
                                partition_spec: String,
                                location_path: String,
                                owner_name: String,
                                external_purge: String)

  /**
   * Glob matching for `scan.tables`, on the shared [[PrimaryUtilities.globToRegexString]] so a
   * pattern means exactly the same thing here and in a purge policy.
   */
  private[purge] def matchesAnyPattern(name: String, patterns: Seq[String]): Boolean =
    patterns.isEmpty || patterns.exists(pattern => globToRegex(pattern).matcher(name).matches())

  private[purge] def globToRegex(pattern: String): java.util.regex.Pattern =
    java.util.regex.Pattern.compile(PrimaryUtilities.globToRegexString(pattern))
}
