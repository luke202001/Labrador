package com.dyingbleed.labrador.report
import com.dyingbleed.labrador.constraint.CheckResult
import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}

/**
  * Created by 李震 on 2017/7/17.
  */
class MailReport(host: String, port: Int, username: String, password: String, from: String, tos: Seq[String]) extends Report {

  override def report(results: Seq[CheckResult]): Unit = {
    val email = new SimpleEmail()

    email.setHostName(host)
    email.setSmtpPort(port)
    email.setAuthenticator(new DefaultAuthenticator(username, password))
    email.setFrom(from)
    email.addTo(tos: _*)

    email.setSubject("Data Quality Report")
    val msg = results.map(result => s"${result.name} ${result.state}").reduce((l1, l2) => l1.concat("\n").concat(l2))
    email.setMsg(msg)

    email.send()
  }

}
