package com.bnp.str.purge.utility

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneId}

/**
 * Retention date arithmetic.
 *
 * Two rules of the house drive the whole file:
 *
 *  1. A retention is expressed as a VALUE + UNIT (8 QUARTER, 10 YEAR), never as a day count.
 *     Converting "10 YEAR" into 3650 days quietly loses leap days and drifts a real regulatory
 *     floor by two or three days, so the cutoff is computed with calendar arithmetic
 *     ([[cutoffDate]]) and the comparison is date-against-date.
 *
 *  2. The age of a partition is preferably read from its BUSINESS date (`as_of_date=2023-03-31`),
 *     not from the file mtime: re-running an engine rewrites the files and would otherwise make a
 *     2023 vintage look like it was produced today — the single most dangerous way to get a purge
 *     decision wrong, in the safe direction here, but wrong.
 */
object DateUtils {

  private val Iso = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val Compact = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val Slashed = DateTimeFormatter.ofPattern("yyyy/MM/dd")

  /** `2025Q2`, `2025-Q2`, `2025q2` — the quarter notation used across the STR engines. */
  private val QuarterPattern = """^(\d{4})-?[Qq]([1-4])$""".r
  /** `202506` / `2025-06` — a month-grained partition. */
  private val MonthPattern = """^(\d{4})-?(0[1-9]|1[0-2])$""".r

  // -------------------------------------------------------------------------------------------
  // retention
  // -------------------------------------------------------------------------------------------

  /**
   * The date before which data is out of retention: anything strictly older than the returned date
   * has served its time. `cutoffDate(2026-08-28, 8, "QUARTER")` = 2024-08-28.
   *
   * @param today          reference date (injected rather than read from the clock, so the rule is testable)
   * @param retentionValue how many units to keep
   * @param retentionUnit  DAY | MONTH | QUARTER | YEAR (case-insensitive)
   * @throws IllegalArgumentException on an unknown unit or a negative value — a retention we cannot
   *                                  interpret must stop the run, never fall back to "delete".
   */
  def cutoffDate(today: LocalDate, retentionValue: Int, retentionUnit: String): LocalDate = {
    require(retentionValue >= 0, s"retentionValue must be >= 0, got $retentionValue")
    val unit = Option(retentionUnit).map(_.trim.toUpperCase).getOrElse("")
    unit match {
      case PrimaryConstants.UNIT_DAY     => today.minusDays(retentionValue.toLong)
      case PrimaryConstants.UNIT_MONTH   => today.minusMonths(retentionValue.toLong)
      case PrimaryConstants.UNIT_QUARTER => today.minusMonths(3L * retentionValue)
      case PrimaryConstants.UNIT_YEAR    => today.minusYears(retentionValue.toLong)
      case other => throw new IllegalArgumentException(
        s"Unknown retention unit '$other'. Expected one of: ${PrimaryConstants.RETENTION_UNITS.mkString(", ")}")
    }
  }

  /** True when `date` is strictly older than the retention cutoff, i.e. eligible on age alone. */
  def isBeyondRetention(date: LocalDate,
                        today: LocalDate,
                        retentionValue: Int,
                        retentionUnit: String): Boolean =
    date.isBefore(cutoffDate(today, retentionValue, retentionUnit))

  /** Whole days between `date` and `today`; negative for a date in the future. */
  def ageInDays(date: LocalDate, today: LocalDate): Long =
    ChronoUnit.DAYS.between(date, today)

  /** Age in days of an HDFS modification time (epoch millis), in the JVM default zone. */
  def ageInDays(epochMillis: Long, today: LocalDate): Long =
    ageInDays(toLocalDate(epochMillis), today)

  /** Epoch millis (an HDFS mtime / atime) as a local date. */
  def toLocalDate(epochMillis: Long, zone: ZoneId = ZoneId.systemDefault()): LocalDate =
    Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDate

  // -------------------------------------------------------------------------------------------
  // business dates found in partition values
  // -------------------------------------------------------------------------------------------

  /**
   * Parse the business date carried by a partition value, in any of the notations the STR engines
   * write: `2023-03-31`, `20230331`, `2023/03/31`, `2023Q1` / `2023-Q1`, `202303` / `2023-03`.
   *
   * A quarter or a month resolves to its LAST day — the vintage is only complete at the end of the
   * period, so its age must be counted from there. Anything unrecognised returns None, and the
   * caller falls back to the file mtime rather than guessing.
   */
  def parseBusinessDate(raw: String): Option[LocalDate] = {
    val s = Option(raw).map(_.trim).getOrElse("")
    if (s.isEmpty) None
    else s match {
      case QuarterPattern(year, quarter) =>
        val lastMonth = quarter.toInt * 3
        Some(LocalDate.of(year.toInt, lastMonth, 1).withDayOfMonth(
          LocalDate.of(year.toInt, lastMonth, 1).lengthOfMonth()))
      case MonthPattern(year, month) =>
        val first = LocalDate.of(year.toInt, month.toInt, 1)
        Some(first.withDayOfMonth(first.lengthOfMonth()))
      case _ =>
        Seq(Iso, Compact, Slashed).view
          .map(fmt => try Some(LocalDate.parse(s, fmt)) catch { case _: Throwable => None })
          .collectFirst { case Some(d) => d }
    }
  }

  // -------------------------------------------------------------------------------------------
  // quarter helpers — same semantics as com.bnp.str.ageing.utility.DateUtils, so a quarter string
  // means the same thing in every module
  // -------------------------------------------------------------------------------------------

  /** `2025Q2` for any date in April–June 2025. */
  def quarterOf(date: LocalDate): String = s"${date.getYear}Q${(date.getMonthValue - 1) / 3 + 1}"

  /** `addQuarters("2025Q3", 2)` = `2026Q1`; a negative `n` walks backwards. */
  def addQuarters(date: String, n: Int): String = {
    val Array(year, quarter) = date.split("[Qq]").map(_.toInt)
    val shifted = (year * 4 + (quarter - 1)) + n
    s"${shifted / 4}Q${shifted % 4 + 1}"
  }

  /** Quarters between two quarter strings: `quarterDiff("2025Q3", "2025Q1")` = 2. */
  def quarterDiff(date1: String, date2: String): Int = {
    val Array(year1, quarter1) = date1.split("[Qq]").map(_.toInt)
    val Array(year2, quarter2) = date2.split("[Qq]").map(_.toInt)
    (year1 * 4 + quarter1) - (year2 * 4 + quarter2)
  }
}
