package com.bnp.str.purge

import com.bnp.str.purge.utility.DateUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class DateUtilsSpec extends AnyFunSuite with Matchers {

  private val today = LocalDate.of(2026, 8, 28)

  // ---- cutoffDate ----------------------------------------------------------------------------

  test("cutoffDate uses calendar arithmetic for every unit") {
    DateUtils.cutoffDate(today, 30, "DAY") shouldBe LocalDate.of(2026, 7, 29)
    DateUtils.cutoffDate(today, 6, "MONTH") shouldBe LocalDate.of(2026, 2, 28)
    DateUtils.cutoffDate(today, 8, "QUARTER") shouldBe LocalDate.of(2024, 8, 28)
    DateUtils.cutoffDate(today, 10, "YEAR") shouldBe LocalDate.of(2016, 8, 28)
  }

  test("cutoffDate crosses a leap day correctly") {
    // 3650 days would land on 2016-08-30 — two days out, which is why days are not used
    DateUtils.cutoffDate(today, 10, "YEAR") shouldBe today.minusYears(10)
    DateUtils.cutoffDate(LocalDate.of(2024, 2, 29), 1, "YEAR") shouldBe LocalDate.of(2023, 2, 28)
  }

  test("cutoffDate is case-insensitive on the unit") {
    DateUtils.cutoffDate(today, 2, "quarter") shouldBe DateUtils.cutoffDate(today, 2, "QUARTER")
  }

  test("cutoffDate refuses an unknown unit rather than guessing") {
    // a retention we cannot interpret must stop the run, never default to "delete"
    val thrown = intercept[IllegalArgumentException](DateUtils.cutoffDate(today, 5, "WEEK"))
    thrown.getMessage should include("WEEK")
  }

  test("cutoffDate refuses a negative retention") {
    intercept[IllegalArgumentException](DateUtils.cutoffDate(today, -1, "DAY"))
  }

  // ---- isBeyondRetention ---------------------------------------------------------------------

  test("isBeyondRetention is exclusive on the cutoff day itself") {
    val cutoff = DateUtils.cutoffDate(today, 8, "QUARTER")
    DateUtils.isBeyondRetention(cutoff, today, 8, "QUARTER") shouldBe false
    DateUtils.isBeyondRetention(cutoff.minusDays(1), today, 8, "QUARTER") shouldBe true
    DateUtils.isBeyondRetention(cutoff.plusDays(1), today, 8, "QUARTER") shouldBe false
  }

  // ---- ageInDays -----------------------------------------------------------------------------

  test("ageInDays counts whole days and goes negative for the future") {
    DateUtils.ageInDays(LocalDate.of(2026, 8, 18), today) shouldBe 10L
    DateUtils.ageInDays(today, today) shouldBe 0L
    DateUtils.ageInDays(LocalDate.of(2026, 9, 1), today) shouldBe -4L
  }

  test("ageInDays accepts an HDFS modification time in epoch millis") {
    val millis = today.minusDays(3).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
    DateUtils.ageInDays(millis, today) shouldBe 3L
  }

  // ---- parseBusinessDate ---------------------------------------------------------------------

  test("parseBusinessDate reads every notation the STR engines write") {
    DateUtils.parseBusinessDate("2023-03-31") shouldBe Some(LocalDate.of(2023, 3, 31))
    DateUtils.parseBusinessDate("20230331") shouldBe Some(LocalDate.of(2023, 3, 31))
    DateUtils.parseBusinessDate("2023/03/31") shouldBe Some(LocalDate.of(2023, 3, 31))
  }

  test("parseBusinessDate resolves a quarter to its last day") {
    // the vintage is only complete at the end of the period, so its age counts from there
    DateUtils.parseBusinessDate("2023Q1") shouldBe Some(LocalDate.of(2023, 3, 31))
    DateUtils.parseBusinessDate("2023-Q4") shouldBe Some(LocalDate.of(2023, 12, 31))
    DateUtils.parseBusinessDate("2024q1") shouldBe Some(LocalDate.of(2024, 3, 31))
  }

  test("parseBusinessDate resolves a month to its last day, leap year included") {
    DateUtils.parseBusinessDate("202302") shouldBe Some(LocalDate.of(2023, 2, 28))
    DateUtils.parseBusinessDate("2024-02") shouldBe Some(LocalDate.of(2024, 2, 29))
  }

  test("parseBusinessDate returns None rather than guessing") {
    DateUtils.parseBusinessDate("latest") shouldBe None
    DateUtils.parseBusinessDate("2023Q5") shouldBe None
    DateUtils.parseBusinessDate("") shouldBe None
    DateUtils.parseBusinessDate(null) shouldBe None
  }

  // ---- quarter helpers -----------------------------------------------------------------------

  test("quarterOf names the quarter of a date") {
    DateUtils.quarterOf(LocalDate.of(2025, 4, 1)) shouldBe "2025Q2"
    DateUtils.quarterOf(LocalDate.of(2025, 12, 31)) shouldBe "2025Q4"
  }

  test("addQuarters and quarterDiff agree with the ageing module") {
    DateUtils.addQuarters("2025Q3", 2) shouldBe "2026Q1"
    DateUtils.addQuarters("2025Q1", -1) shouldBe "2024Q4"
    DateUtils.quarterDiff("2025Q3", "2025Q1") shouldBe 2
    DateUtils.quarterDiff("2024Q4", "2025Q2") shouldBe -2
  }
}
