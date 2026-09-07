package com.bnp.str.purge

import com.bnp.str.purge.reader.PolicyReader
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PolicyReaderSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private def conf(rules: String, source: String = "conf"): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  ${PrimaryConstants.PURGE_POLICY} {
         |    source = "$source"
         |    rules = [ $rules ]
         |  }
         |}""".stripMargin)

  private val validRule =
    """{ policyId = "P-RWA", domain = "RWA", pathPattern = "/data/promethee/str/rwa/*",
      |  dateColumn = "as_of_date", retentionValue = 8, retentionUnit = "QUARTER",
      |  keepMinVersions = 4, strategy = "TRASH", ownerGroup = "grp_str_rwa" }""".stripMargin

  private def read(rules: String) =
    new PolicyReader()(spark, conf(rules)).read()

  // ---- what a policy becomes -------------------------------------------------------------------

  test("a rule is read into the columns the selection SQL joins on") {
    val row = read(validRule).head()

    row.getAs[String]("policy_id") shouldBe "P-RWA"
    row.getAs[Int]("retention_value") shouldBe 8
    row.getAs[String]("retention_unit") shouldBe "QUARTER"
    row.getAs[String]("date_column") shouldBe "as_of_date"
    row.getAs[Boolean]("legal_hold") shouldBe false
  }

  test("the path pattern is compiled to an anchored regex and its literal prefix measured") {
    val row = read(validRule).head()

    val regex = row.getAs[String]("path_regex")
    "/data/promethee/str/rwa/ts_ead_fwd".matches(regex) shouldBe true
    // the sibling directory that a plain prefix test would wrongly accept
    "/data/promethee/str/rwa-archive/t".matches(regex) shouldBe false

    // "/data/promethee/str/rwa/" — what makes "longest pattern wins" a defined rule
    row.getAs[Int]("prefix_length") shouldBe 24
  }

  test("a more specific policy has a longer prefix than the domain catch-all above it") {
    val rules =
      """{ policyId = "P-DOMAIN", pathPattern = "/data/promethee/str/*",
        |  retentionValue = 2, retentionUnit = "YEAR" },
        |{ policyId = "P-TABLE", pathPattern = "/data/promethee/str/rwa/ts_ead_fwd/*",
        |  retentionValue = 8, retentionUnit = "QUARTER" }""".stripMargin

    val byId = read(rules).collect().map(r => r.getAs[String]("policy_id") -> r.getAs[Int]("prefix_length")).toMap
    byId("P-TABLE") should be > byId("P-DOMAIN")
  }

  test("a pattern is canonicalised the way every path is, so spelling does not create a second rule") {
    val rules = """{ policyId = "P", pathPattern = "hdfs://nn/data//promethee/str/rwa/*",
                  |  retentionValue = 1, retentionUnit = "YEAR" }""".stripMargin
    read(rules).head().getAs[String]("path_pattern") shouldBe "/data/promethee/str/rwa/*"
  }

  test("a pattern without a wildcard matches exactly that path") {
    val rules = """{ policyId = "P", pathPattern = "/data/promethee/str/rwa/ts_ead_fwd",
                  |  retentionValue = 1, retentionUnit = "YEAR" }""".stripMargin
    val row = read(rules).head()

    "/data/promethee/str/rwa/ts_ead_fwd".matches(row.getAs[String]("path_regex")) shouldBe true
    "/data/promethee/str/rwa/ts_ead_fwd/x".matches(row.getAs[String]("path_regex")) shouldBe false
  }

  test("a relative pattern is resolved the way the filesystem resolves it") {
    // the inventory always reports absolute paths; a relative pattern left as written would match
    // nothing, and "matched nothing" is indistinguishable from "there was nothing to purge"
    val rules = """{ policyId = "P", pathPattern = "localRun/purge/input/datalake/*",
                  |  retentionValue = 1, retentionUnit = "YEAR" }""".stripMargin
    val pattern = read(rules).head().getAs[String]("path_pattern")

    pattern should startWith("/")
    pattern should endWith("/localRun/purge/input/datalake/*")
  }

  test("an already absolute pattern is left as it is") {
    read(validRule).head().getAs[String]("path_pattern") shouldBe "/data/promethee/str/rwa/*"
  }

  // ---- validation: a policy we cannot interpret must stop the run ------------------------------

  test("an unknown retention unit is refused") {
    val rules = """{ policyId = "P", pathPattern = "/data/a/b/c/*", retentionValue = 5, retentionUnit = "WEEK" }"""
    intercept[IllegalArgumentException](read(rules)).getMessage should include("WEEK")
  }

  test("a missing retention value is refused rather than defaulted") {
    // the only sane default would be "no retention", and "no retention" reads as "delete it"
    val rules = """{ policyId = "P", pathPattern = "/data/a/b/c/*", retentionUnit = "YEAR" }"""
    intercept[IllegalArgumentException](read(rules)).getMessage should include("retentionValue")
  }

  test("a missing policyId is refused") {
    val rules = """{ pathPattern = "/data/a/b/c/*", retentionValue = 1, retentionUnit = "YEAR" }"""
    intercept[IllegalArgumentException](read(rules)).getMessage should include("policyId")
  }

  test("a policy that names no scope at all is refused") {
    val rules = """{ policyId = "P", retentionValue = 1, retentionUnit = "YEAR" }"""
    intercept[IllegalArgumentException](read(rules)).getMessage should include("pathPattern")
  }

  test("an unknown strategy is refused") {
    val rules = """{ policyId = "P", pathPattern = "/data/a/b/c/*", retentionValue = 1,
                  |  retentionUnit = "YEAR", strategy = "SHRED" }""".stripMargin
    intercept[IllegalArgumentException](read(rules)).getMessage should include("SHRED")
  }

  test("archiveRequired without an archiveRoot is refused, because PC07 could never pass") {
    val rules = """{ policyId = "P", pathPattern = "/data/a/b/c/*", retentionValue = 1,
                  |  retentionUnit = "YEAR", archiveRequired = true }""".stripMargin
    intercept[IllegalArgumentException](read(rules)).getMessage should include("archiveRoot")
  }

  test("a duplicated policyId is refused") {
    val rules =
      """{ policyId = "P", pathPattern = "/data/a/b/c/*", retentionValue = 1, retentionUnit = "YEAR" },
        |{ policyId = "P", pathPattern = "/data/d/e/f/*", retentionValue = 2, retentionUnit = "YEAR" }""".stripMargin
    intercept[IllegalArgumentException](read(rules)).getMessage should include("declared 2 times")
  }

  test("every problem is listed at once, so the referential is fixed in one pass") {
    val rules =
      """{ policyId = "", pathPattern = "/data/a/b/c/*", retentionValue = 1, retentionUnit = "DECADE" }"""
    val message = intercept[IllegalArgumentException](read(rules)).getMessage
    message should include("policyId")
    message should include("DECADE")
  }

  // ---- sources ----------------------------------------------------------------------------------

  test("an empty referential loads without error and selects nothing") {
    read("").count() shouldBe 0L
  }

  test("an unknown source is refused") {
    val config = ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { ${PrimaryConstants.PURGE_POLICY} { source = "yaml" } }""")
    intercept[IllegalArgumentException](new PolicyReader()(spark, config).read())
      .getMessage should include("yaml")
  }

  test("source = table without a table name is refused") {
    val config = ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { ${PrimaryConstants.PURGE_POLICY} { source = "table" } }""")
    intercept[IllegalArgumentException](new PolicyReader()(spark, config).read())
      .getMessage should include("table is not set")
  }
}
