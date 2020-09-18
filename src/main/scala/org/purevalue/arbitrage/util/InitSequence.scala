package org.purevalue.arbitrage.util

import org.purevalue.arbitrage.Main.actorSystem
import org.slf4j.Logger

import scala.concurrent.ExecutionContextExecutor

case class InitStep(name: String, method: () => Unit)
case class InitStepFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause)

class InitSequence(private val parentLogger: Logger,
                   private val sequence: List[InitStep]) {
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  def run(): Unit = {
    var position: Int = 0
    for (step <- sequence) {
      position += 1
      try {
        parentLogger.info(s"starting init step [$position/${sequence.size}] '${step.name}'")
        step.method.apply()
      } catch {
        case t: Throwable => throw InitStepFailedException(s"Init step [$position/${sequence.size}] '${step.name}' failed", t)
      }
    }
    parentLogger.info(s"""init sequence completed""")
  }
}
