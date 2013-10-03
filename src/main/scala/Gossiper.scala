/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/27/13
 * Time: 5:36 PM
 */

import akka.actor.Actor
import scala.util.Random

abstract class Gossiper(val topology: Topology, val termCnt: Int) extends Actor {
  var idx: Int = 0
  var curCnt: Int = 0
  val random = new Random
  var iNeighbors: Array[Int] = _

  override def preStart() = {
    // TODO some other more specific way to calculate a seed
    random.setSeed(this.hashCode.toLong)
  }

  protected def iNextNeighbor: Int = iNeighbors(random.nextInt(iNeighbors.length))

  private def genericMessageHandler: Receive = {
    case Init(_idx, _iNeighbors) =>
      idx = _idx
      iNeighbors = _iNeighbors
      sender ! Ready
  }

  protected def specificMessageHandler: Receive

  def receive = specificMessageHandler.orElse(genericMessageHandler)

}

class GossipGossiper(topology: Topology, termCnt: Int)
  extends Gossiper(topology, termCnt) {

  def specificMessageHandler: Receive = {
    case Content =>
      // once every Gossiper has sent a Done msg then system terminates
      if (curCnt==0) context.parent ! Done

      // system also terminates when any Gossiper gets content 10 times
      curCnt += 1
      if (curCnt < termCnt) {
        self ! Send
      } else if (curCnt == termCnt) {
        println("Gossiper#" + this.idx + " triggers termination")
        context.parent ! Term
      }
    case Send =>
      val in = iNextNeighbor
      topology.gossiper(in) ! Content
  }
}

class PushSumGossiper(topology: Topology, termCnt: Int)
  extends Gossiper(topology, termCnt) {
  private var s: Double = idx
  private var w: Double = 1
  private val isConverging: Array[Boolean] = Array.fill(termCnt)(false)

  def specificMessageHandler: Receive = {
    case Content(_s, _w) =>
      curCnt += 1
      isConverging(curCnt % termCnt) = scala.math.abs(s / w - (s + _s) / (w + _w)) < 1e-10
      s += _s
      w += _w
      if (isConverging forall (_ == true)) {
        println("Gossiper#" + this.idx + " triggers termination")
        context.parent ! Result(s/w)
        context.parent ! Term
      }
      self ! Send
    case Send =>
      s /= 2.0
      w /= 2.0
      topology.gossiper(iNextNeighbor) ! Content(s, w)
  }

}

object GossiperFactory {
  def create(s: String, termCnt: Int): (Topology) => Gossiper = {
    s match {
      case "gossip" =>
        (topology: Topology) =>
          if (termCnt>0) new GossipGossiper(topology, termCnt)
          else new GossipGossiper(topology, 10)
      case "push-sum" =>
        (topology: Topology) =>
          if (termCnt>0) new PushSumGossiper(topology, termCnt)
          else new PushSumGossiper(topology, 3)
    }
  }
}