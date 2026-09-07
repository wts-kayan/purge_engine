package com.bnp.str.utilities.audit

import java.sql.Timestamp

/** Status of the Spark application run recorded in run_history. */
object RunStatus {
  final val RUNNING = "RUNNING" // set at start; a row left RUNNING = a run whose JVM was killed / crashed
  final val SUCCESS = "SUCCESS"
  final val FAILED  = "FAILED"
}

/**
 * One row of the `run_history` audit — a single execution of ANY module packaged in the jar.
 * Persisted as an ORC-backed external Hive table, PARTITIONED BY (module_name, run_id) — see
 * [[RunAuditStore]].
 *
 * SCHEMA (column : type — meaning)
 * ---------------------------------------------------------------------------------------------
 *  run_id           : string       unique id of the run (PARTITION); generated or given in the conf
 *  application_id   : string       Spark application id, e.g. application_1773889567248_10449
 *  module_name      : string       module that ran (PARTITION): addons / climatetables / excelor / tseadfwd
 *  used_jar         : string       jar the run was launched from, e.g. str-file-transform-engine-1.0-RELEASE-Climate-Tables.jar
 *  used_conf        : string       path of the application.conf used
 *  user_launcher    : string       user who launched the run (given in the conf), e.g. j03627
 *  status           : string       Spark run state: RUNNING | SUCCESS | FAILED
 *  creation_date    : timestamp    run start
 *  end_date         : timestamp?   run end (null while running)
 *  duration         : string?      human-readable elapsed time, e.g. "0h 20mn 27s" (null while running)
 *  motor            : string       compute motor/engine, e.g. iris
 *  projection_dates : string?      projection years, e.g. "[2030, 2040, 2050]" (module-specific)
 *  scenarios        : string?      scenarios, e.g. "[\"FW\", \"NZ50\", \"DT\", \"NDC\"]" (module-specific)
 *  base_folder_name : string?      run base folder, e.g. "ICAAP_TR_2025Q1_251016_v1" (module-specific)
 * ---------------------------------------------------------------------------------------------
 */
case class RunAuditRecord(
                           runId: String,
                           applicationId: String,
                           moduleName: String,
                           usedJar: String,
                           usedConf: String,
                           userLauncher: String,
                           status: String,
                           creationDate: Timestamp,
                           endDate: Option[Timestamp],
                           duration: Option[String],
                           motor: String,
                           projectionDates: Option[String],
                           scenarios: Option[String],
                           baseFolderName: Option[String]
                         )
