package com.Knoldus.sinks

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.JavaConversions._

abstract class ElasticsearchUpsertSink[T](host: String, port: Int, cluster: String, index: String, mapping: String)
  extends RichSinkFunction[T] {

  private var client: TransportClient = null

  def insertJson(record: T): Map[String, AnyRef]

  def updateJson(record: T): Map[String, AnyRef]

  def indexKey(record: T): String

  @throws[Exception]
  override def open(parameters: Configuration) {

    val config = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", cluster)

    val settings = ImmutableSettings.settingsBuilder()
      .put(config)
      .build()
    client = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(host, port))
  }

  @throws[Exception]
  override def invoke(r: T) {
    // do an upsert request to elastic search

    // index document if it does not exist
    val indexRequest = new IndexRequest(index, mapping, indexKey(r))
      .source(mapAsJavaMap(insertJson(r)))

    // update document if it exists
    val updateRequest = new UpdateRequest(index, mapping, indexKey(r))
      .doc(mapAsJavaMap(updateJson(r)))
      .upsert(indexRequest)

    client.update(updateRequest).get()
  }

}
