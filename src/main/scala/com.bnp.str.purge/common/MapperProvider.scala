package com.bnp.str.purge.common

import org.apache.spark.sql.DataFrame

abstract class MapperProvider() extends Serializable {

  def getDataFrame_purge: DataFrame

  def getMapping_purge: DataFrame = {
    getDataFrame_purge
  }

}
