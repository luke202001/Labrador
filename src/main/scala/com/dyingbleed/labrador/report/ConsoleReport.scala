package com.dyingbleed.labrador.report
import com.dyingbleed.labrador.constraint.CheckResult

/**
  * Created by 李震 on 2017/7/14.
  */
class ConsoleReport extends Report {

  override def report(results: Seq[CheckResult]): Unit = {
    results.foreach(result => {
      println(s"${result.name} ${result.state}")
    })
  }

}
