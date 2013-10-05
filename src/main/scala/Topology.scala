/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/27/13
 * Time: 5:30 PM
 */

import akka.actor.{Props, ActorRef, Actor}
import scala.util.Random
import scala.math.{sqrt, abs}

abstract class Topology(val nGossipers: Int, val createGossiper: (Topology) => Gossiper) extends Actor {

  private val gossipers = new Array[ActorRef](nGossipers)

  private var nCurGossipers: Int = _
  private var startTime: Long = _

  // set read-only from outside
  var isTerminating: Boolean = _

  private var sSum: Double = _
  private var wSum: Double = _
  private var resExpected: Double = _ // expected result for Push-Sum

  protected def iNeighbors(iGossiper: Int): Array[Int]

  def gossiper(iGossiper: Int): ActorRef = {
    gossipers(iGossiper)
  }

  def receive = {
    case Start =>
      println("build topology")
      for (i <- 0 until nGossipers) {
        gossipers(i) = context.actorOf(Props(createGossiper(this)))
      }
      for (i <- 0 until nGossipers) {
        gossipers(i) ! Init(i, iNeighbors(i))
      }
    case Ready =>
      nCurGossipers += 1
      if (nCurGossipers == nGossipers) {
        println("start protocol")
        startTime = System.currentTimeMillis
        gossipers(Random.nextInt(nGossipers)) ! Content
      }

    case First => // Gossip
      nCurGossipers -= 1
      if (nCurGossipers == 0 && !isTerminating) {
        self ! Term
      }

    case First(s, w) => // Push-Sum
      sSum += s
      wSum += w
      nCurGossipers -= 1
      if (nCurGossipers == 0) {
        resExpected = sSum / wSum
      }

    case Term =>
      if (!isTerminating) {
        isTerminating = true
        println("shutdown system")
        println("propagate ratio " + (nGossipers - nCurGossipers) + "/" + nGossipers)
        println("cost time " + (System.currentTimeMillis - startTime) + " ms")
        context.system.shutdown()
      }

    case Result(res) =>
      // Push-Sum expected result is sum of all initial s divided by sum of all initial w
      if (resExpected == 0) {
        resExpected = sSum / wSum
        println("expected result " + resExpected + " (based on " + (nGossipers - nCurGossipers) + " gossipers)")
      } else {
        println("expected result " + resExpected)
      }
      println("converging result " + res)
  }
}

class FullTopology(nGossipers: Int, createGossiper: (Topology) => Gossiper)
  extends Topology(nGossipers, createGossiper) {
  override def iNeighbors(iGossipers: Int): Array[Int] = {
    Array.range(0, nGossipers).filterNot(_ == iGossipers)
  }
}

class LineTopology(nGossipers: Int, createGossiper: (Topology) => Gossiper)
  extends Topology(nGossipers, createGossiper) {
  override def iNeighbors(iGossiper: Int): Array[Int] =
    Array(iGossiper - 1, iGossiper + 1).filterNot(i => i < 0 || i >= nGossipers)
}

// here the nGossipers has already been rounded up
class GridTopology(nGossipers: Int, createGossiper: (Topology) => Gossiper) extends Topology(nGossipers, createGossiper) {

  val lenSide: Int = sqrt(nGossipers).toInt

  def iLRUD(iGossiper: Int): Array[Int] = {
    val iRow: Int = iGossiper / lenSide
    val iCol: Int = iGossiper % lenSide
    val iLeft: Int = if (iCol == 0) -1 else iGossiper - 1
    val iRight: Int = if (iCol == lenSide - 1) nGossipers else iGossiper + 1
    val iUp: Int = iGossiper - lenSide
    val iDown: Int = iGossiper + lenSide
    Array(iLeft, iRight, iUp, iDown)
  }

  override def iNeighbors(iGossiper: Int): Array[Int] = {
    iLRUD(iGossiper).filterNot(i => i < 0 || i >= nGossipers)
  }
}

class ImperfectGridTopology(nGossipers: Int, createGossiper: (Topology) => Gossiper) extends GridTopology(nGossipers, createGossiper) {

  val random = new Random(System.currentTimeMillis())

  override def iNeighbors(iGossiper: Int): Array[Int] = {
    val iGridNeighbors = super.iNeighbors(iGossiper)
    val iRandNeighbors = Array.range(0, nGossipers).filterNot(i => iGridNeighbors.contains(i) || i == iGossiper)
    val iRandNeighbor = iRandNeighbors(random.nextInt(iRandNeighbors.length))
    iGridNeighbors :+ iRandNeighbor
  }
}

object TopologyFactory {
  private def roundSquare(num: Int): Int = {
    val sqrtVal = sqrt(num)
    if (sqrtVal.toInt == sqrtVal) num
    else {
      val ceil = sqrtVal.ceil.toInt
      val floor = sqrtVal.floor.toInt
      if (abs(ceil - sqrtVal) < abs(floor - sqrtVal)) ceil * ceil
      else floor * floor
    }
  }

  def create(tType: String, termCnt: Int): (Int, String) => Topology = {
    tType match {
      case "full" =>
        (nGossipers: Int, gType: String) => new FullTopology(nGossipers, GossiperFactory.create(gType, termCnt))
      case "line" =>
        (nGossipers: Int, gType: String) => new LineTopology(nGossipers, GossiperFactory.create(gType, termCnt))
      case "2D" =>
        (nGossipers: Int, gType: String) => new GridTopology(roundSquare(nGossipers), GossiperFactory.create(gType, termCnt))
      case "imp2D" =>
        (nGossipers: Int, gType: String) => new ImperfectGridTopology(roundSquare(nGossipers), GossiperFactory.create(gType, termCnt))
    }
  }
}

