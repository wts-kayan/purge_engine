package com.bnp.str.purge

import com.bnp.str.purge.reader.CatalogReader
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CatalogReaderSpec extends AnyFunSuite with Matchers with SparkTestSession {

  // ---- the table filter ------------------------------------------------------------------------

  test("an empty pattern list matches every table") {
    CatalogReader.matchesAnyPattern("ts_ead_fwd", Seq.empty) shouldBe true
  }

  test("a plain name matches only itself, case-insensitively") {
    CatalogReader.matchesAnyPattern("ts_ead_fwd", Seq("ts_ead_fwd")) shouldBe true
    CatalogReader.matchesAnyPattern("TS_EAD_FWD", Seq("ts_ead_fwd")) shouldBe true
    CatalogReader.matchesAnyPattern("ts_ead_fwd_old", Seq("ts_ead_fwd")) shouldBe false
  }

  test("* and ? are the only wildcards, and they are anchored") {
    CatalogReader.matchesAnyPattern("ts_ead_fwd", Seq("ts_*")) shouldBe true
    CatalogReader.matchesAnyPattern("crr_param", Seq("ts_*")) shouldBe false
    CatalogReader.matchesAnyPattern("t1", Seq("t?")) shouldBe true
    CatalogReader.matchesAnyPattern("t12", Seq("t?")) shouldBe false
  }

  test("a regex metacharacter in a table name matches itself and does not widen the scope") {
    // '.' must not become "any character" — that is how a filter silently selects extra tables
    CatalogReader.matchesAnyPattern("tsXead", Seq("ts.ead")) shouldBe false
    CatalogReader.matchesAnyPattern("ts.ead", Seq("ts.ead")) shouldBe true
  }

  test("any pattern of the list is enough") {
    CatalogReader.matchesAnyPattern("crr_param", Seq("ts_*", "crr_*")) shouldBe true
  }

  // ---- the reader itself -----------------------------------------------------------------------

  test("an empty database list yields an empty catalog rather than an error") {
    // a path-only purge scope is a legitimate way to run the engine
    val config = ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [], roots = [], databases = [], tables = [] }
         |}""".stripMargin)

    new CatalogReader()(spark, config).read().count() shouldBe 0L
  }

  test("a database that does not exist is skipped with a warning, not a failure") {
    val config = ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [], roots = [], databases = ["no_such_db"], tables = [] }
         |}""".stripMargin)

    new CatalogReader()(spark, config).read().count() shouldBe 0L
  }
}
