package com.Knoldus.datatypes

import java.util.Locale

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class KnolxSession(
                    var sessionId: Long,
                    var sessionType: String,
                    var sessionDate: DateTime,
                    var sessionName: String,
                    var audienceCount: Long,
                    var isMeetup: Boolean) {

  def this() {
    this(0, "", new DateTime(0), "", 0, false)
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(sessionId).append(",")
    sb.append(sessionType).append(",")
    sb.append(sessionDate.toString(KnolxSession.TimeFormatter)).append(",")
    sb.append(sessionName).append(",")
    sb.append(audienceCount).append(",")
    sb.append(isMeetup)
    sb.toString()
  }

}

object KnolxSession {

  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC

  def fromString(line: String): KnolxSession = {
    val tokens: Array[String] = line.split(",")
    if (tokens.length != 6) {
      throw new RuntimeException("Invalid record: " + line)
    }
    try {
      val sessionId = tokens(0).toLong
      val sessionType = tokens(1).toString //DateTime.parse(tokens(1), TimeFormatter)
      val sessionDate = DateTime.parse(tokens(2), TimeFormatter)
      val sessionName = tokens(3).toString
      val audienceCount = tokens(4).toLong
      val isMeetup = tokens(5).toBoolean

      new KnolxSession(sessionId, sessionType, sessionDate, sessionName, audienceCount, isMeetup)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}
