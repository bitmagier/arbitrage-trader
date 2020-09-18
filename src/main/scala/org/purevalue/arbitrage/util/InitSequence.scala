package org.purevalue.arbitrage.util

import org.purevalue.arbitrage.Main.actorSystem
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor

case class InitStep(name: String, method: () => Unit)
case class InitStepFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause)

class InitSequence(private val sequence: List[InitStep],
                   private val onComplete: () => Unit,
                   private val onFailure: InitStepFailedException => Unit) {
  private val log = LoggerFactory.getLogger(classOf[InitSequence])
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  def run(): Unit = {
    log.debug(s"""starting init sequence [${sequence.mkString(",")}]""")
    var position:Int = 0
    for (step <- sequence) {
      position += 1
      try {
        log.debug(s"starting init step [$position] '${step.name}'")
        step.method.apply()
      } catch {
        case t: Throwable =>
          val msg = s"Init step [$position] '${step.name}' failed"
          log.debug(msg, t)
          onFailure(InitStepFailedException(msg, t))
      }
    }
    log.debug(s"""init sequence [${sequence.mkString(",")}] completed""")
    onComplete()
  }
}
