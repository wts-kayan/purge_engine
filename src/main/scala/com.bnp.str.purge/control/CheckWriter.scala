package com.bnp.str.purge.control

import com.bnp.str.purge.utility.PrimaryUtilities
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Persists the purge control report. */
object CheckWriter {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Write the HTML report to `path`, which goes through Hadoop's FileSystem — so the same call takes
   * a local path (`localRun/...`) or an HDFS one (`hdfs:///user/...`) with no change.
   *
   * Failing to write the report FAILS the run, unlike the audit writes elsewhere in the codebase.
   * The report is not a diagnostic here: it is the document the approval is given on. A simulation
   * that produced no readable report has produced nothing an approver may act on, and finishing
   * quietly would leave a manifest that looks ready for approval and is not.
   */
  def writeHtml(path: String, report: CheckReport)(implicit spark: SparkSession): Unit = {
    require(path.nonEmpty, "controls.htmlPath is not set: there is nowhere to write the control report")
    PrimaryUtilities.writeStringToHdfs(path, CheckHtmlView.render(report))(spark.sparkContext)
    log.info(s"Control report written -> $path")
  }
}
