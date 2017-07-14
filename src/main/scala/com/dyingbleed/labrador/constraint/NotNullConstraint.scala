package com.dyingbleed.labrador.constraint
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2017/7/14.
  */
class NotNullConstraint(column: String) extends Constraint {

  override def check(df: DataFrame): CheckResult = {
    val count = df.filter(s"${column} IS NULL OR length(${column}) = 0 ").count()
    if (count > 0) new CheckResult(s"${column} ${NotNullConstraint.NAME}", CheckResult.FAILED)
    else new CheckResult(s"${column} ${NotNullConstraint.NAME}", CheckResult.SUCCESS)
  }

}


object NotNullConstraint {

  val NAME = "notnull"

}