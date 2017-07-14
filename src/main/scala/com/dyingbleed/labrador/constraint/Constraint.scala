package com.dyingbleed.labrador.constraint

import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2017/7/14.
  */
trait Constraint {

  def check(df: DataFrame): CheckResult

}
