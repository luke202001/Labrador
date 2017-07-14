package com.dyingbleed.labrador.constraint
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2017/7/14.
  */
class CountConstraint(f: Long => Boolean) extends Constraint {

  override def check(df: DataFrame): CheckResult = {
    val count = df.count()
    if (f(count)) new CheckResult(CountConstraint.NAME, CheckResult.SUCCESS)
    else new CheckResult(CountConstraint.NAME, CheckResult.FAILED)
  }

}

object CountConstraint {

  val NAME = "count"

}