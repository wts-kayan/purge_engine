package com.bnp.str.purge.control

import com.bnp.str.purge.utility.PrimaryUtilities

/**
 * Renders the control report as one self-contained HTML file — no external stylesheet, no script,
 * nothing to fetch. It is opened from TWIST, attached to a change request, and read months later by
 * an auditor on a machine with no access to this cluster, so it has to carry everything it needs.
 *
 * Pure string building over [[CheckReport]]: no Spark, no IO, which is what lets the rendering be
 * tested against a golden expectation rather than eyeballed.
 */
object CheckHtmlView {

  def render(report: CheckReport): String =
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="utf-8">
       |<title>Purge control report ${escape(displayRequest(report))}</title>
       |<style>
       |${style()}
       |</style>
       |</head>
       |<body>
       |<main>
       |${header(report)}
       |${unknownBanner(report)}
       |${counters(report)}
       |${matrix(report)}
       |${findings(report)}
       |${footer(report)}
       |</main>
       |</body>
       |</html>
       |""".stripMargin

  // ---------------------------------------------------------------------------------------------

  private def header(report: CheckReport): String =
    s"""<header>
       |  <p class="eyebrow">Purge engine &middot; control report</p>
       |  <h1>${escape(displayRequest(report))}</h1>
       |  <p class="verdict ${verdictClass(report)}">${escape(report.verdict)}</p>
       |  <dl class="meta">
       |    <div><dt>Scope</dt><dd>${escape(report.source)}</dd></div>
       |    <div><dt>Run</dt><dd>${escape(report.runId)}</dd></div>
       |    <div><dt>As of</dt><dd>${escape(report.asOfDate)}</dd></div>
       |    <div><dt>Generated</dt><dd>${escape(report.generatedAt)}</dd></div>
       |    <div><dt>Manifest fingerprint</dt><dd class="mono">${escape(report.manifestFingerprint)}</dd></div>
       |  </dl>
       |  <p class="notice">Nothing has been deleted. This report describes what a purge
       |  <em>would</em> do; the deletion happens only after a second person approves this exact
       |  manifest.</p>
       |</header>""".stripMargin

  /**
   * The controls that could not be checked, hoisted above everything else.
   *
   * An approver reading a green report needs to know which greens are "we checked" and which are
   * "we could not check" before reading anything else — that difference is the one most likely to
   * change their decision, and the one a table of thirteen rows hides best.
   */
  private def unknownBanner(report: CheckReport): String = {
    val unknowns = report.unknowns
    if (unknowns.isEmpty) return ""
    s"""<section class="banner">
       |  <h2>${unknowns.size} control(s) could not be checked</h2>
       |  <p>These controls did not pass &mdash; they did not run, because the data they need was not
       |  available to this run. Read the rest of this report knowing they are silent.</p>
       |  <ul>
       |${unknowns.map(r => s"""    <li><b>${escape(r.rule.id)}</b> ${escape(r.rule.title)} &mdash; ${escape(r.action)}</li>""").mkString("\n")}
       |  </ul>
       |</section>""".stripMargin
  }

  private def counters(report: CheckReport): String =
    s"""<section class="counters">
       |  <div class="counter"><span class="n">${report.candidates}</span><span class="l">candidates</span></div>
       |  <div class="counter ok"><span class="n">${report.toDelete}</span><span class="l">deletable</span></div>
       |  <div class="counter warn"><span class="n">${report.toWarn}</span><span class="l">with warning</span></div>
       |  <div class="counter block"><span class="n">${report.blocked}</span><span class="l">blocked</span></div>
       |  <div class="counter"><span class="n">${escape(PrimaryUtilities.humanBytes(report.bytesToFree))}</span><span class="l">would be freed</span></div>
       |  <div class="counter"><span class="n">${escape(PrimaryUtilities.humanBytes(report.bytesTotal))}</span><span class="l">examined</span></div>
       |</section>""".stripMargin

  private def matrix(report: CheckReport): String =
    s"""<section>
       |  <h2>Controls</h2>
       |  <table class="matrix">
       |    <thead><tr><th>Id</th><th>Control</th><th>Type</th><th>Status</th><th>Objects</th><th>Action</th></tr></thead>
       |    <tbody>
       |${report.results.map(matrixRow).mkString("\n")}
       |    </tbody>
       |  </table>
       |</section>""".stripMargin

  private def matrixRow(result: CheckRuleResult): String = {
    val kind = if (result.rule.blocking) "blocking" else "warning"
    val locked = if (result.rule.locked) """ <span class="lock" title="cannot be switched off">locked</span>""" else ""
    s"""      <tr class="${statusClass(result)}">
       |        <td class="mono">${escape(result.rule.id)}</td>
       |        <td><b>${escape(result.rule.title)}</b>$locked<p class="detail">${escape(result.rule.detail)}</p></td>
       |        <td>${escape(kind)}</td>
       |        <td class="status">${escape(result.status)}</td>
       |        <td class="num">${if (result.total == 0L) "&mdash;" else result.total.toString}</td>
       |        <td>${escape(result.action)}</td>
       |      </tr>""".stripMargin
  }

  private def findings(report: CheckReport): String = {
    val fired = report.results.filter(_.total > 0L)
    if (fired.isEmpty)
      return """<section><h2>Findings</h2><p class="empty">No control fired on any candidate.</p></section>"""

    s"""<section>
       |  <h2>Findings</h2>
       |${fired.map(findingBlock).mkString("\n")}
       |</section>""".stripMargin
  }

  private def findingBlock(result: CheckRuleResult): String = {
    val truncated =
      if (!result.truncated) ""
      else s"""  <p class="truncated">Showing the ${result.findings.size} largest of ${result.total};
              |  the manifest holds them all.</p>""".stripMargin

    s"""  <article class="finding ${if (result.rule.blocking) "blocking" else "warning"}">
       |    <h3><span class="mono">${escape(result.rule.id)}</span> ${escape(result.rule.title)}
       |      <span class="badge">${escape(result.status)}</span>
       |      <span class="bytes">${escape(PrimaryUtilities.humanBytes(result.bytes))}</span></h3>
       |$truncated
       |    <table class="findings">
       |      <thead><tr><th>Object</th><th>Table</th><th>Why</th><th>Size</th></tr></thead>
       |      <tbody>
       |${result.findings.map(findingRow).mkString("\n")}
       |      </tbody>
       |    </table>
       |  </article>""".stripMargin
  }

  private def findingRow(finding: PurgeFinding): String =
    s"""        <tr>
       |          <td class="mono path">${escape(finding.path)}</td>
       |          <td>${escape(finding.table)}</td>
       |          <td>${escape(finding.value)}</td>
       |          <td class="num">${escape(PrimaryUtilities.humanBytes(finding.sizeBytes))}</td>
       |        </tr>""".stripMargin

  private def footer(report: CheckReport): String =
    s"""<footer>
       |  <p>${escape(report.summaryLine)}</p>
       |  <p>Generated by the purge engine (com.bnp.str.purge) for run
       |  <span class="mono">${escape(report.runId)}</span>.</p>
       |</footer>""".stripMargin

  // ---------------------------------------------------------------------------------------------

  private def displayRequest(report: CheckReport): String =
    if (report.requestId.nonEmpty) report.requestId else s"run ${report.runId}"

  private def verdictClass(report: CheckReport): String = report.verdict match {
    case "CLEAR" => "ok"
    case "WARNINGS" => "warn"
    case "BLOCKED" => "block"
    case _ => "none"
  }

  private def statusClass(result: CheckRuleResult): String =
    result.status match {
      case "PASS" => "row-ok"
      case "UNKNOWN" => "row-unknown"
      case s if s.startsWith("BLOCKED") => "row-block"
      case s if s.startsWith("WARNED") => "row-warn"
      case _ => "row-muted"
    }

  /** Every value in this report comes from a path, a policy or a config someone typed. */
  private[purge] def escape(raw: String): String =
    Option(raw).getOrElse("")
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")

  private def style(): String =
    """  :root {
      |    --ink: #1a1614; --ink-2: #4a423e; --ink-3: #7c716c;
      |    --line: #e2dcd9; --surface: #ffffff; --ground: #f5f3f2;
      |    --block: #9b3a31; --block-bg: #f2e3e0;
      |    --warn: #9a6a0c; --warn-bg: #f4ebd8;
      |    --ok: #2e6b4e; --ok-bg: #e2ede7;
      |  }
      |  * { box-sizing: border-box; }
      |  body { margin: 0; background: var(--ground); color: var(--ink);
      |         font-family: "IBM Plex Sans", "Segoe UI", system-ui, sans-serif;
      |         font-size: 15px; line-height: 1.6; }
      |  main { max-width: 76rem; margin: 0 auto; padding: 2rem 1.5rem 4rem; }
      |  h1 { font-size: 2rem; margin: .2rem 0 .6rem; }
      |  h2 { font-size: 1.25rem; margin: 2.5rem 0 .8rem; }
      |  h3 { font-size: 1rem; margin: 0 0 .5rem; display: flex; gap: .6rem; align-items: baseline;
      |       flex-wrap: wrap; }
      |  p { margin: .4rem 0; }
      |  .mono { font-family: "IBM Plex Mono", ui-monospace, Consolas, monospace; font-size: .86em; }
      |  .eyebrow { text-transform: uppercase; letter-spacing: .14em; font-size: .72rem;
      |             color: var(--ink-3); margin: 0; }
      |  .verdict { display: inline-block; font-weight: 600; letter-spacing: .08em;
      |             padding: .25rem .7rem; border-radius: 3px; font-size: .8rem; }
      |  .verdict.ok { color: var(--ok); background: var(--ok-bg); }
      |  .verdict.warn { color: var(--warn); background: var(--warn-bg); }
      |  .verdict.block { color: var(--block); background: var(--block-bg); }
      |  .verdict.none { color: var(--ink-3); background: var(--line); }
      |  .meta { display: flex; flex-wrap: wrap; gap: 0 2rem; border-top: 1px solid var(--line);
      |          margin-top: 1.2rem; padding-top: .9rem; }
      |  .meta div { padding: .3rem 0; }
      |  .meta dt { text-transform: uppercase; letter-spacing: .1em; font-size: .66rem;
      |             color: var(--ink-3); }
      |  .meta dd { margin: 0; font-size: .88rem; }
      |  .notice { background: var(--surface); border: 1px solid var(--line); border-left: 3px solid var(--ok);
      |            padding: .8rem 1rem; margin-top: 1.2rem; color: var(--ink-2); }
      |  .banner { background: var(--warn-bg); border: 1px solid var(--warn); border-radius: 4px;
      |            padding: 1rem 1.2rem; margin-top: 1.5rem; }
      |  .banner h2 { margin: 0 0 .3rem; font-size: 1.05rem; color: var(--warn); }
      |  .banner ul { margin: .5rem 0 0; padding-left: 1.1rem; }
      |  .counters { display: flex; flex-wrap: wrap; gap: .6rem; margin-top: 1.6rem; }
      |  .counter { flex: 1 1 8rem; background: var(--surface); border: 1px solid var(--line);
      |             border-radius: 4px; padding: .8rem .9rem; display: flex; flex-direction: column; }
      |  .counter .n { font-size: 1.5rem; font-weight: 600; font-variant-numeric: tabular-nums; }
      |  .counter .l { font-size: .72rem; text-transform: uppercase; letter-spacing: .1em;
      |                color: var(--ink-3); }
      |  .counter.ok .n { color: var(--ok); }
      |  .counter.warn .n { color: var(--warn); }
      |  .counter.block .n { color: var(--block); }
      |  table { border-collapse: collapse; width: 100%; background: var(--surface);
      |          border: 1px solid var(--line); border-radius: 4px; }
      |  th { text-align: left; font-size: .68rem; text-transform: uppercase; letter-spacing: .1em;
      |       color: var(--ink-3); padding: .6rem .8rem; border-bottom: 1px solid var(--line);
      |       background: var(--ground); white-space: nowrap; }
      |  td { padding: .6rem .8rem; border-bottom: 1px solid var(--line); vertical-align: top;
      |       color: var(--ink-2); }
      |  tr:last-child td { border-bottom: none; }
      |  .num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
      |  .status { font-weight: 600; white-space: nowrap; }
      |  .detail { font-size: .8rem; color: var(--ink-3); margin: .3rem 0 0; }
      |  .row-block .status { color: var(--block); }
      |  .row-warn .status { color: var(--warn); }
      |  .row-ok .status { color: var(--ok); }
      |  .row-unknown { background: var(--warn-bg); }
      |  .row-unknown .status { color: var(--warn); }
      |  .row-muted .status { color: var(--ink-3); }
      |  .lock { font-size: .62rem; text-transform: uppercase; letter-spacing: .1em;
      |          color: var(--block); background: var(--block-bg); padding: .1rem .35rem;
      |          border-radius: 2px; margin-left: .4rem; }
      |  .finding { margin: 1.2rem 0; }
      |  .finding .badge { font-size: .68rem; letter-spacing: .08em; padding: .1rem .4rem;
      |                    border-radius: 2px; }
      |  .finding.blocking .badge { color: var(--block); background: var(--block-bg); }
      |  .finding.warning .badge { color: var(--warn); background: var(--warn-bg); }
      |  .finding .bytes { font-size: .78rem; color: var(--ink-3); }
      |  .findings .path { word-break: break-all; }
      |  .truncated { font-size: .8rem; color: var(--ink-3); }
      |  .empty { color: var(--ink-3); }
      |  footer { margin-top: 3rem; border-top: 1px solid var(--line); padding-top: 1rem;
      |           font-size: .82rem; color: var(--ink-3); }""".stripMargin
}
