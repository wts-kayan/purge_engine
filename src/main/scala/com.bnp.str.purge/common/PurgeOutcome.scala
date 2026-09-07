package com.bnp.str.purge.common

import com.bnp.str.purge.control.CheckReport
import org.apache.spark.sql.DataFrame

/**
 * What one simulation produced: the judged manifest, the report that explains it, and the
 * fingerprint the approval will be computed over.
 *
 * The three travel together because they are only meaningful together — a manifest without its
 * report cannot be approved, and a report without its fingerprint cannot be tied to the manifest it
 * describes.
 */
final case class PurgeOutcome(manifest: DataFrame,
                              report: CheckReport,
                              manifestFingerprint: String)
