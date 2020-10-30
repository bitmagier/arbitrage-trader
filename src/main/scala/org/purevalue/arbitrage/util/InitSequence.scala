package org.purevalue.arbitrage.util

import org.purevalue.arbitrage.Main.actorSystem
import org.slf4j.Logger

import scala.concurrent.ExecutionContextExecutor

case class InitStep(name: String, method: () => Unit)
case class InitStepFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause)

class InitSequence(private val parentLogger: Logger,
                   private val instanceName: String,
                   private val sequence: List[InitStep]) {
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.executionContext

  def run(): Unit = {
    var position: Int = 0
    for (step <- sequence) {
      position += 1
      try {
        parentLogger.info(s"[$instanceName]: starting init step [$position/${sequence.size}] '${step.name}'")
        step.method()
      } catch {
        case t: Throwable => throw InitStepFailedException(s"[$instanceName] Init step [$position/${sequence.size}] '${step.name}' failed", t)
      }
    }
    parentLogger.info(s"""[$instanceName] init sequence completed""")
  }
}
