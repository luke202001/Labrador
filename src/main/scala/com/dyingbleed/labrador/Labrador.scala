package com.dyingbleed.labrador

import com.dyingbleed.labrador.constraint._
import com.dyingbleed.labrador.report.Report
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2017/7/14.
  */
class Labrador(df: DataFrame,
               constraints: Seq[Constraint] = Seq.empty) {

  def count(f: Long => Boolean): Labrador = {
    new Labrador(df, constraints ++ List(new CountConstraint(f)))
  }

  def notNull(column: String): Labrador = {
    new Labrador(df, constraints ++ List(new NotNullConstraint(column)))
  }

  def regexp(column: String, regexp: String): Labrador = {
    new Labrador(df, constraints ++ List(new RegexpConstraint(column, regexp)))
  }

  def run(reports: Report*): Unit = {
    val results = constraints.map(constraints => constraints.check(df))

    for (report <- reports) {
      report.report(results)
    }
  }

}


object Labrador {

  def create(df: DataFrame): Labrador = {
    new Labrador(df)
  }

}