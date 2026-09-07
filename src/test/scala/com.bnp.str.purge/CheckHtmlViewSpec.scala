package com.bnp.str.purge

import com.bnp.str.purge.control._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The report is read by an approver, and months later by an auditor, on a machine with no access to
 * this cluster. These tests hold it to that: self-contained, honest about what was not checked, and
 * safe with the arbitrary strings it renders — every value in it comes from a path, a policy or a
 * config that someone typed.
 */
class CheckHtmlViewSpec extends AnyFunSuite with Matchers {

  private def result(rule: PurgeRule,
                     total: Long = 0L,
                     enabled: Boolean = true,
                     notEvaluated: Option[String] = None,
                     findings: Seq[PurgeFinding] = Seq.empty) =
    CheckRuleResult(rule, enabled, notEvaluated, total, findings, bytes = total * 100L)

  private def report(results: Seq[CheckRuleResult],
                     candidates: Long = 10L,
                     toDelete: Long = 8L,
                     toWarn: Long = 1L,
                     blocked: Long = 1L) =
    CheckReport(
      source = "/data/promethee/str/rwa",
      runId = "run-1", requestId = "PRG-2026-000142",
      generatedAt = "2026-08-28 21:00:00", asOfDate = "2026-08-28",
      manifestFingerprint = "abc123",
      candidates = candidates, toDelete = toDelete, toWarn = toWarn, blocked = blocked,
      bytesTotal = 1024L, bytesToFree = 512L,
      results = results)

  private val allPassing = PurgeRule.All.map(rule => result(rule))

  // ---- the document ------------------------------------------------------------------------------

  test("the report is a self-contained page with no external reference") {
    val html = CheckHtmlView.render(report(allPassing))

    html should startWith("<!DOCTYPE html>")
    html should include("</html>")
    // it has to open on a machine that cannot reach this cluster, or anything else
    html should not include "http://"
    html should not include "https://"
    html should not include "<script"
  }

  test("the header carries what ties the report to its manifest") {
    val html = CheckHtmlView.render(report(allPassing))

    html should include("PRG-2026-000142")
    html should include("run-1")
    html should include("abc123")
    html should include("/data/promethee/str/rwa")
    html should include("2026-08-28")
  }

  test("the report says plainly that nothing was deleted") {
    CheckHtmlView.render(report(allPassing)) should include("Nothing has been deleted")
  }

  // ---- the verdict -------------------------------------------------------------------------------

  test("the verdict reflects what the controls found") {
    CheckHtmlView.render(report(allPassing, blocked = 0, toWarn = 0, toDelete = 10)) should
      include("""<p class="verdict ok">CLEAR</p>""")
    CheckHtmlView.render(report(allPassing, blocked = 0, toWarn = 2, toDelete = 8)) should
      include("""<p class="verdict warn">WARNINGS</p>""")
    CheckHtmlView.render(report(allPassing)) should include("""<p class="verdict block">BLOCKED</p>""")
  }

  // ---- unknowns ----------------------------------------------------------------------------------

  test("controls that could not be checked are hoisted above everything else") {
    // the difference an approver most needs to see, and the one a table of thirteen rows hides best
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.ActiveRun) result(rule, notEvaluated = Some(NotEvaluated.NO_DATA))
      else result(rule)
    }
    val html = CheckHtmlView.render(report(results))

    html should include("1 control(s) could not be checked")
    html.indexOf("could not be checked") should be < html.indexOf("<h2>Controls</h2>")
    html should include("PC04")
  }

  test("no banner appears when every control produced a verdict") {
    CheckHtmlView.render(report(allPassing)) should not include "could not be checked"
  }

  test("an unknown control is never rendered as passing") {
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.DownstreamDependency) result(rule, notEvaluated = Some(NotEvaluated.NO_DATA))
      else result(rule)
    }
    val html = CheckHtmlView.render(report(results))

    html should include("UNKNOWN")
    html should include("row-unknown")
  }

  // ---- the matrix and the findings ---------------------------------------------------------------

  test("every control appears in the matrix with its status") {
    val html = CheckHtmlView.render(report(allPassing))
    PurgeRule.All.foreach(rule => html should include(rule.id))
  }

  test("a locked control is marked as such") {
    val html = CheckHtmlView.render(report(allPassing))
    html should include("""title="cannot be switched off"""")
  }

  test("findings are listed for the controls that fired") {
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.LegalHold)
        result(rule, total = 1L, findings = Seq(
          PurgeFinding("/data/promethee/str/ifrs9/t/as_of_date=2019-12-31", "dbiris.t",
            "policy P-IFRS9-REG carries a legal hold", 2048L)))
      else result(rule)
    }
    val html = CheckHtmlView.render(report(results))

    html should include("/data/promethee/str/ifrs9/t/as_of_date=2019-12-31")
    html should include("policy P-IFRS9-REG carries a legal hold")
    html should include("2.0 KiB")
  }

  test("a truncated finding list says so") {
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.LegalHold)
        result(rule, total = 500L, findings = Seq(PurgeFinding("/data/rwa/t/p=1", "t", "held", 1L)))
      else result(rule)
    }
    CheckHtmlView.render(report(results)) should include("Showing the 1 largest of 500")
  }

  test("a report where nothing fired says so instead of showing an empty table") {
    CheckHtmlView.render(report(allPassing)) should include("No control fired on any candidate")
  }

  // ---- escaping ------------------------------------------------------------------------------------

  test("a path containing markup is escaped, not rendered") {
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.LegalHold)
        result(rule, total = 1L, findings = Seq(
          PurgeFinding("/data/rwa/<script>alert('x')</script>", "t & co", "\"quoted\"", 1L)))
      else result(rule)
    }
    val html = CheckHtmlView.render(report(results))

    html should not include "<script>alert"
    html should include("&lt;script&gt;")
    html should include("t &amp; co")
    html should include("&quot;quoted&quot;")
  }

  test("escape handles every character that could break out of the document") {
    CheckHtmlView.escape("""<a href="x" title='y'> & </a>""") shouldBe
      "&lt;a href=&quot;x&quot; title=&#39;y&#39;&gt; &amp; &lt;/a&gt;"
  }

  test("escape is null-safe, because a finding value may legitimately be absent") {
    CheckHtmlView.escape(null) shouldBe ""
  }

  // ---- writing it ------------------------------------------------------------------------------------

  test("the written report is the rendered document, with no checksum sidecar beside it") {
    // the reports directory is browsed by the person about to approve a purge
    val dir = java.nio.file.Files.createTempDirectory("purge-report-spec")
    try {
      val target = dir.resolve("PRG-1.html").toString
      CheckWriter.writeHtml(target, report(allPassing))(SparkTestSession.instance)

      val written = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(target)), "UTF-8")
      written shouldBe CheckHtmlView.render(report(allPassing))
      java.nio.file.Files.exists(dir.resolve(".PRG-1.html.crc")) shouldBe false
    } finally org.apache.commons.io.FileUtils.deleteQuietly(dir.toFile)
  }

  test("writing to nowhere is refused, rather than leaving an unreviewable manifest") {
    intercept[IllegalArgumentException](
      CheckWriter.writeHtml("", report(allPassing))(SparkTestSession.instance))
  }

  // ---- the model behind it -------------------------------------------------------------------------

  test("a rule result that could not run never reports PASS") {
    result(PurgeRule.ActiveRun, notEvaluated = Some(NotEvaluated.NO_DATA)).status shouldBe "UNKNOWN"
    result(PurgeRule.ManifestDrift, notEvaluated = Some(NotEvaluated.EXECUTE_PHASE)).status shouldBe "NOT EVALUATED"
    result(PurgeRule.LegalHold, enabled = false, notEvaluated = Some(NotEvaluated.DISABLED)).status shouldBe "SKIPPED"
    result(PurgeRule.LegalHold).status shouldBe "PASS"
  }

  test("a blocking and a warning hit are described differently") {
    result(PurgeRule.LegalHold, total = 3L).status shouldBe "BLOCKED(3)"
    result(PurgeRule.RecentlyAccessed, total = 3L).status shouldBe "WARNED(3)"
    result(PurgeRule.LegalHold, total = 3L).action should include("removed from what may be deleted")
    result(PurgeRule.RecentlyAccessed, total = 3L).action should include("still deletable")
  }

  test("the summary line names the controls that could not be checked") {
    val results = PurgeRule.All.map { rule =>
      if (rule == PurgeRule.ActiveRun) result(rule, notEvaluated = Some(NotEvaluated.NO_DATA))
      else result(rule)
    }
    val line = report(results).summaryLine

    line should include("could not be checked: PC04")
    line should include("nothing deleted")
  }
}
