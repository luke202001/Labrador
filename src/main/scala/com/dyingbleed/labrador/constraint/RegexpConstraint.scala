package com.dyingbleed.labrador.constraint
import java.util.regex.Pattern

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by 李震 on 2017/7/14.
  */
class RegexpConstraint(column: String, regexp: String) extends Constraint {

  override def check(df: DataFrame): CheckResult = {
    val pattern = Pattern.compile(regexp)
    val notMatch = udf((column: String) => {
      column != null && !pattern.matcher(column).find()
    })

    val count = df.filter(notMatch(col(column))).count()
    if (count > 0) new CheckResult(s"${column} ${RegexpConstraint.NAME} ${regexp}", CheckResult.FAILED)
    else new CheckResult(s"${column} ${RegexpConstraint.NAME} ${regexp}", CheckResult.SUCCESS)
  }

}


object RegexpConstraint {

  val NAME = "regexp"

}