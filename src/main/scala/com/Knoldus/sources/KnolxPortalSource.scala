package com.Knoldus.sources

import java.io._
import java.util.zip.GZIPInputStream
import java.util.{Calendar, Random}

import com.Knoldus.datatypes.KnolxSession
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable


class KnolxPortalSource(dataFilePath: String, maxDelaySecs: Int, servingSpeed: Float)
  extends EventTimeSourceFunction[KnolxSession] {

  private val maxDelayMsecs = maxDelaySecs * 1000
  private val watermarkDelayMSecs = if (maxDelayMsecs < 10000) 10000 else maxDelayMsecs

  @transient
  private var reader: BufferedReader = null
  @transient
  private var gzipStream: InputStream = null

  override def run(sourceContext: SourceContext[KnolxSession]): Unit = {
    gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath))
    reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))

    if (this.maxDelayMsecs == 0) {
      generateOrderedStream(sourceContext)
    }
    else {
      generateUnorderedStream(sourceContext)
    }

    this.reader.close()
    this.reader = null
    this.gzipStream.close()
    this.gzipStream = null
  }

  @throws(classOf[IOException])
  override def cancel(): Unit = {
    try {
      if (this.reader != null) {
        this.reader.close()
      }
      if (this.gzipStream != null) {
        this.gzipStream.close()
      }
    } finally {
      this.reader = null
      this.gzipStream = null
    }
  }

  @throws(classOf[Exception])
  private def generateOrderedStream(sourceContext: SourceContext[KnolxSession]) {

    val servingStartTime = Calendar.getInstance.getTimeInMillis
    var dataStartTime = 0L
    var nextWatermark = 0L
    var nextWatermarkServingTime = 0L

    // read the first ride event
    if (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        print(line)
        val ride = KnolxSession.fromString(line)

        // set time of first event
        dataStartTime = ride.sessionDate.getMillis
        // initialize watermarks
        nextWatermark = dataStartTime + watermarkDelayMSecs
        nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        // emit first event
        sourceContext.collectWithTimestamp(ride, ride.sessionDate.getMillis)
      }
    }
    else {
      return
    }

    // read all following ride events
    while (reader.ready) {
      val line = reader.readLine
      if (line != null) {

        // read event
        val ride = KnolxSession.fromString(line)
        val eventTime = ride.sessionDate.getMillis
        val now = Calendar.getInstance.getTimeInMillis
        val eventServingTime = toServingTime(servingStartTime, dataStartTime, eventTime)

        // get time to wait until event and next watermark needs to be emitted
        val eventWait = eventServingTime - now
        val watermarkWait = nextWatermarkServingTime - now

        if (eventWait < watermarkWait) {
          // wait to emit next event
          Thread.sleep(if (eventWait > 0) eventWait else 0)
        }
        else if (eventWait > watermarkWait) {
          // wait to emit watermark
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)
          // emit watermark
          sourceContext.emitWatermark(new Watermark(nextWatermark))
          // schedule next watermark
          nextWatermark = nextWatermark + watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
          // wait to emit event
          val remainWait: Long = eventWait - watermarkWait
          Thread.sleep(if (remainWait > 0) remainWait else 0)
        }
        else if (eventWait == watermarkWait) {
          // wait to emit watermark
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)
          // emit watermark
          sourceContext.emitWatermark(new Watermark(nextWatermark - 1))
          // schedule next watermark
          nextWatermark = nextWatermark + watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        }
        // emit event
        sourceContext.collectWithTimestamp(ride, ride.sessionDate.getMillis)
      }
    }
  }

  @throws(classOf[Exception])
  private def generateUnorderedStream(sourceContext: SourceContext[KnolxSession]) {

    val servingStartTime = Calendar.getInstance.getTimeInMillis
    var dataStartTime = 0L
    val rand: Random = new Random(7452)

    val emitSchedule = mutable.PriorityQueue.empty[(Long, Either[KnolxSession, Watermark])](
      Ordering.by( (_: (Long, Either[KnolxSession, Watermark]))._1 ).reverse
    )

    var ride: KnolxSession = null
    if (reader.ready) {

      val line = reader.readLine
      if (line != null) {

        ride = KnolxSession.fromString(line)
        dataStartTime = ride.sessionDate.getMillis

        // schedule first event
        val delayedEventTime: Long = dataStartTime + getNormalDelayMsecs(rand)
        emitSchedule += ( (delayedEventTime, Left(ride)) )
        // schedule first watermark
        val watermarkTime = dataStartTime + watermarkDelayMSecs
        val nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1)
        emitSchedule += ( (watermarkTime, Right(nextWatermark)) )
      }
    }
    else {
      return
    }

    if (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        ride = KnolxSession.fromString(line)
      }
    }

    while (emitSchedule.nonEmpty || reader.ready) {

      // insert all events into schedule that might be emitted next
      val curNextDelayedEventTime = if (emitSchedule.nonEmpty) emitSchedule.head._1 else -1
      var rideEventTime = if (ride != null) ride.sessionDate.getMillis else -1
      while (ride != null &&
        (emitSchedule.isEmpty || (rideEventTime < curNextDelayedEventTime + maxDelayMsecs))) {

        // insert event into schedule
        val delayedEventTime = rideEventTime + getNormalDelayMsecs(rand)
        emitSchedule += ( (delayedEventTime, Left(ride)) )

        // read next ride from input
        if (reader.ready) {
          val line = reader.readLine
          if (line != null) {
            ride = KnolxSession.fromString(line)
            rideEventTime = ride.sessionDate.getMillis
          } else {
            ride = null
            rideEventTime = -1
          }
        } else {
          ride = null
          rideEventTime = -1
        }
      }

      // emit next element
      val head = emitSchedule.dequeue()
      val delayedEventTime = head._1

      val now = Calendar.getInstance.getTimeInMillis
      val servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime)
      val waitTime = servingTime - now

      Thread.sleep(if (waitTime > 0) waitTime else 0)

      head._2 match {
        case Left(emitRide) => {
          // emit event
          sourceContext.collectWithTimestamp(emitRide, emitRide.sessionDate.getMillis)
        }
        case Right(emitWatermark) => {
          // emit watermark
          sourceContext.emitWatermark(emitWatermark)
          // schedule next watermark
          val watermarkTime = delayedEventTime + watermarkDelayMSecs
          val nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1)
          emitSchedule += ( (watermarkTime, Right(nextWatermark)))
        }
      }
    }
  }

  def toServingTime(servingStartTime: Long, dataStartTime: Long, eventTime: Long): Long = {
    val dataDiff = eventTime - dataStartTime
    servingStartTime + (dataDiff / this.servingSpeed).toLong
  }

  def getNormalDelayMsecs(rand: Random): Long = {
    var delay = -1L
    val x = maxDelayMsecs / 2
    while (delay < 0 || delay > maxDelayMsecs) {
      delay = (rand.nextGaussian * x).toLong + x
    }
    delay
  }

}
