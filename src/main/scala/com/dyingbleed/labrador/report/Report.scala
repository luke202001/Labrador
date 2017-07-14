package com.dyingbleed.labrador.report

import com.dyingbleed.labrador.constraint.CheckResult

/**
  * Created by 李震 on 2017/7/14.
  */
trait Report {

  def report(results: Seq[CheckResult]): Unit

}
