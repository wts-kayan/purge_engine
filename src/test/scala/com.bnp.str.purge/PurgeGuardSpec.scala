package com.bnp.str.purge

import com.bnp.str.purge.job.MainDriver
import com.bnp.str.purge.purge.PurgeGuard
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

/**
 * The gate that cannot be switched off.
 *
 * Every one of these conditions is also checked by a control, and the guard exists precisely for the
 * run where that control was disabled by someone who did not understand it. So the tests here do not
 * configure the controls at all: they hand the guard a manifest that a control should have stopped,
 * and check that it stops anyway.
 */
class PurgeGuardSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val allowedRoot = "/data/promethee/str/rwa"

  private def conf(guard: String = "", roots: String = allowedRoot, request: String = ""): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [ "$roots" ], roots = [ "$roots" ], databases = [] }
         |  request { id = "PRG-1", requester = "j03627" $request }
         |  guard { $guard }
         |}""".stripMargin)

  private def manifest(rows: GuardRow*): DataFrame = {
    import spark.implicits._
    spark.createDataset(rows.toList).toDF()
  }

  private def row(path: String,
                  decision: String = PrimaryConstants.DECISION_DELETE,
                  sizeBytes: Long = 100L,
                  generatedAt: Timestamp = new Timestamp(System.currentTimeMillis())): GuardRow =
    GuardRow(path, decision, sizeBytes, "fingerprint-abcdef0123456789", generatedAt)

  private def assertRefused(m: DataFrame, config: Config): String =
    intercept[IllegalStateException](PurgeGuard.assertSafe(m)(spark, config)).getMessage

  // ---- what it lets through ------------------------------------------------------------------------

  test("a manifest of legitimate targets passes") {
    PurgeGuard.assertSafe(manifest(row(s"$allowedRoot/ts_ead_fwd/runId=abc")))(spark, conf())
  }

  test("an empty manifest passes: there is nothing to refuse") {
    PurgeGuard.assertSafe(manifest())(spark, conf())
  }

  test("a blocked row is not a target, so its path is not judged") {
    // the executor never touches it, so an unsafe path on a blocked row must not abort the run
    PurgeGuard.assertSafe(
      manifest(row("/tmp", decision = PrimaryConstants.DECISION_BLOCKED)))(spark, conf())
  }

  // ---- containment -----------------------------------------------------------------------------------

  test("a target outside the allowed roots refuses the run") {
    assertRefused(manifest(row("/data/promethee/str/ifrs9/t/runId=abc")), conf()) should
      include("outside the allowed roots")
  }

  test("a target on the forbidden list refuses the run") {
    assertRefused(manifest(row("/tmp")), conf(roots = "/")) should include("forbidden")
  }

  test("a target shallower than the minimum depth refuses the run") {
    assertRefused(manifest(row("/data/rwa")), conf(roots = "/data")) should include("depth")
  }

  test("a target still carrying a wildcard refuses the run") {
    assertRefused(manifest(row(s"$allowedRoot/*")), conf()) should include("wildcard")
  }

  test("a target with a traversal refuses the run") {
    assertRefused(manifest(row(s"$allowedRoot/../../../etc")), conf()) should include("..")
  }

  test("the whole run is refused, not the offending row") {
    // a manifest containing one impossible target is not a manifest to partially trust
    val message = assertRefused(
      manifest(row(s"$allowedRoot/good/runId=abc"), row("/tmp")), conf())
    message should include("refuses this run")
  }

  test("every unsafe target is named, so the conf is fixed in one pass") {
    val message = assertRefused(manifest(row("/data/a/b/c/one"), row("/data/a/b/c/two")), conf())
    message should include("one")
    message should include("two")
  }

  // ---- ceilings ----------------------------------------------------------------------------------------

  test("more objects than the ceiling refuses the run") {
    val rows = (1 to 5).map(i => row(s"$allowedRoot/t/runId=$i"))
    assertRefused(manifest(rows: _*), conf(guard = "maxObjects = 4")) should include("ceiling of 4")
  }

  test("more bytes than the ceiling refuses the run") {
    assertRefused(manifest(row(s"$allowedRoot/t/runId=a", sizeBytes = 5000L)),
      conf(guard = "maxBytes = 1000")) should include("exceeds the ceiling")
  }

  test("the ceilings are counted over the deletable rows only") {
    val rows = Seq(row(s"$allowedRoot/t/runId=a"),
      row(s"$allowedRoot/t/runId=b", decision = PrimaryConstants.DECISION_BLOCKED))
    PurgeGuard.assertSafe(manifest(rows: _*))(spark, conf(guard = "maxObjects = 1"))
  }

  // ---- staleness ----------------------------------------------------------------------------------------

  test("a manifest older than the limit refuses the run") {
    // the drift check catches what changed, never what was created since
    val old = new Timestamp(System.currentTimeMillis() - 100L * 3600L * 1000L)
    assertRefused(manifest(row(s"$allowedRoot/t/runId=a", generatedAt = old)),
      conf(guard = "manifestMaxAgeHours = 72")) should include("re-run the simulation")
  }

  test("a fresh manifest passes the age check") {
    PurgeGuard.assertSafe(manifest(row(s"$allowedRoot/t/runId=a")))(spark, conf(guard = "manifestMaxAgeHours = 72"))
  }

  test("manifestMaxAgeHours = 0 switches the age check off") {
    val old = new Timestamp(System.currentTimeMillis() - 100L * 3600L * 1000L)
    PurgeGuard.assertSafe(manifest(row(s"$allowedRoot/t/runId=a", generatedAt = old)))(
      spark, conf(guard = "manifestMaxAgeHours = 0"))
  }

  // ---- approval, for a perimeter that still wants one ------------------------------------------------------

  test("no approval is required by default, per the business decision") {
    PurgeGuard.assertSafe(manifest(row(s"$allowedRoot/t/runId=a")))(spark, conf())
  }

  test("with requireApproval on, a missing token refuses the run") {
    assertRefused(manifest(row(s"$allowedRoot/t/runId=a")), conf(guard = "requireApproval = true")) should
      include("approvalToken")
  }

  test("with requireApproval on, the approver may not be the requester") {
    val message = assertRefused(manifest(row(s"$allowedRoot/t/runId=a")),
      conf(guard = "requireApproval = true",
        request = """, approver = "j03627", approvalToken = "fingerprint-abcdef0123456789" """))
    message should include("same person")
  }

  test("with requireApproval on, a token for another manifest refuses the run") {
    val message = assertRefused(manifest(row(s"$allowedRoot/t/runId=a")),
      conf(guard = "requireApproval = true",
        request = """, approver = "m12345", approvalToken = "token-for-a-different-manifest" """))
    message should include("does not carry the fingerprint")
  }

  test("with requireApproval on, a matching token passes") {
    PurgeGuard.assertSafe(manifest(row(s"$allowedRoot/t/runId=a")))(
      spark, conf(guard = "requireApproval = true",
        request = """, approver = "m12345", approvalToken = "sig:fingerprint-abcdef0123456789" """))
  }

  // ---- the mode ---------------------------------------------------------------------------------------------

  test("the default mode is the one that deletes nothing") {
    MainDriver.modeOf(ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { request { id = "PRG-1" } }""")) shouldBe MainDriver.MODE_SIMULATE
  }

  test("a mistyped mode is refused, never interpreted") {
    // neither silently EXECUTE nor silently SIMULATE: a typo is corrected, not guessed at
    intercept[IllegalArgumentException](MainDriver.modeOf(ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { request { mode = "EXECUT" } }"""))).getMessage should include("EXECUT")
  }

  test("the modes are recognised whatever their casing") {
    MainDriver.modeOf(ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { request { mode = "execute" } }""")) shouldBe MainDriver.MODE_EXECUTE
  }
}

/** The manifest columns the guard reads. */
final case class GuardRow(path: String,
                          decision: String,
                          size_bytes: Long,
                          manifest_fingerprint: String,
                          generated_at: Timestamp)
