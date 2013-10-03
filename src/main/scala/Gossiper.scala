/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/27/13
 * Time: 5:36 PM
 */

import akka.actor.{ActorRef, Actor}
import scala.util.Random

abstract class Gossiper(val termCnt: Int) extends Actor {
  var id: Int = 0
  var curCnt: Int = 0
  val random = new Random
  var neighbors: Array[ActorRef] = _

  override def preStart() = {
    // TODO some other more specific way to calculate a seed
    random.setSeed(this.hashCode.toLong)
  }

  override def postStop() = {
    neighbors foreach (_ ! Remove(self))
  }

  def nextNeighbor: ActorRef =
  // TODO how to solve dead lock??? how about some one not even get one msg at all
    if (neighbors.isEmpty) { println(id + "'s next is self")
      self
    }
    else {
      val r = random.nextInt(neighbors.length)
      println(id + "'s next is " )
      neighbors(r)
    }

  def genericMessageHandler: Receive = {
    case Remove(removeMe) =>
      println(id+" updated neighbors")
      neighbors = neighbors filterNot (_ equals removeMe)
    case Init(_id, _neighbors) =>
      id = _id
      neighbors = _neighbors
      sender ! Ready
  }

  def specificMessageHandler: Receive

  def receive = specificMessageHandler.orElse(genericMessageHandler)

}

class GossipGossiper(val _termCnt: Int = 10)
  extends Gossiper(_termCnt) {

  var isDone = false

  def specificMessageHandler: Receive = {
    case Content =>
      curCnt += 1
      println(id+"\t"+curCnt)
      if (curCnt == termCnt) {
        println(this.id + " terminates")
        context.parent ! Done
        isDone = true
//        context.stop(self)
      }
      self ! Spread
    case Spread =>
      if (!isDone)
      nextNeighbor ! Content
//      self ! Wait
    case Wait =>
//      wait(500)
      self ! Spread
  }

}

class PushSumGossiper(val _termCnt: Int = 3)
  extends Gossiper(_termCnt) {
  private var s: Double = id
  private var w: Double = 1
  private val isConverging: Array[Boolean] = Array.fill(termCnt)(false)

  def specificMessageHandler: Receive = {
    case Spread =>
      s /= 2.0
      w /= 2.0
      nextNeighbor ! Content(s, w)
      self ! Wait
    case Content(_s, _w) =>
      curCnt += 1
      isConverging(curCnt % termCnt) = scala.math.abs(s / w - (s + _s) / (w + _w)) < 1e-10
      s += _s
      w += _w
      if (isConverging forall (_ == true)) {
        println(this.id + " terminates")
        context.stop(self)
      }
      // TODO exit(FinalSum(s / w))
      else self ! Wait
    case Wait =>
//      wait(500)
      self ! Spread
  }

}

object GossiperFactory {
  def create(s: String): () => Gossiper = {
    s match {
      case "gossip" => () => new GossipGossiper()
      case "pushsum" => () => new PushSumGossiper()
    }
  }
}