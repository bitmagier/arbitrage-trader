package org.purevalue.arbitrage.adapter

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, SourceQueueWithComplete}
import akka.stream.{FlowShape, Graph, SinkShape}

import scala.concurrent.Promise

//object StreamGraph {
//  def webSocketPlusQueueSourceToSink[W, H, T](queueSource: Source[T, SourceQueueWithComplete[T]],
//                                              downstreamSink: Sink[T, NotUsed]): Graph[SinkShape[Message], Message] = {
//    import akka.stream.scaladsl.GraphDSL.Implicits._
//    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
//      val WS: FlowShape[Message, Message] = builder.add(websocketFlow)
//      val WSmap: FlowShape[Message, T]
//
//      WS ~> WSmap
//
//
//    })
//  }
//
//  def websocketFlow[I](subscribeMessages:List[TextMessage], graph:Graph[SinkShape[Message], Message]): Flow[Message, Message, Promise[Option[Message]]] =
//    Flow.fromSinkAndSourceCoupledMat(
//    graph,
//    Source(subscribeMessages).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
//
//}
