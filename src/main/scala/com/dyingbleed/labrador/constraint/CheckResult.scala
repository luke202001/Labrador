package com.dyingbleed.labrador.constraint

/**
  * Created by 李震 on 2017/7/14.
  */
case class CheckResult(name: String, state: String)

object CheckResult {

  val SUCCESS = "success"

  val FAILED = "failed"

}