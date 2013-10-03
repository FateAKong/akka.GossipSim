/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/27/13
 * Time: 5:30 PM
 */

import akka.actor.{Props, ActorRef, Actor}
import scala.util.Random
import scala.math.{sqrt, abs}

abstract class Topology(val nGossipers: Int, val createGossiper: () => Gossiper) extends Actor {

  val gossipers = new Array[ActorRef](nGossipers)

  var nCurGossipers = 0

  def neighbors(iGossiper: Int): Array[ActorRef]

  def receive = {
    case Start =>
      println(this.toString() + "#Start")
      for (i <- 0 until nGossipers) {
        gossipers(i) = context.actorOf(Props(createGossiper()))
      }
      for (i <- 0 until nGossipers) {
        gossipers(i) ! Init(i, neighbors(i))
      }
    case Ready =>
      nCurGossipers += 1
      if (nCurGossipers == nGossipers) {
        println(this.toString() + "#Ready")
        gossipers(Random.nextInt(nGossipers)) ! Spread
      }
    case Done =>
      nCurGossipers -= 1
      println(nCurGossipers + "/" + nGossipers + "still working")
      if (nCurGossipers == 0) {
        println(this.toString() + "#Done")
        context.system.shutdown()
      }

  }
}

class FullTopology(nGossipers: Int, createGossiper: () => Gossiper)
  extends Topology(nGossipers, createGossiper) {
  override def neighbors(iGossipers: Int): Array[ActorRef] = {
    Array.range(0, nGossipers).filterNot(_ == iGossipers).map(gossipers(_))
  }
}

class LineTopology(nGossipers: Int, createGossiper: () => Gossiper)
  extends Topology(nGossipers, createGossiper) {
  override def neighbors(iGossiper: Int): Array[ActorRef] =
    Array(iGossiper - 1, iGossiper + 1).filterNot(i => i < 0 || i >= nGossipers).map(gossipers(_))
}

// here the nGossipers has already been rounded up
class GridTopology(nGossipers: Int, createGossiper: () => Gossiper) extends Topology(nGossipers, createGossiper) {

  val lenSide: Int = sqrt(nGossipers).toInt

  def iNeighbors(iGossiper: Int): Array[Int] = {
    val iRow: Int = iGossiper / lenSide
    val iCol: Int = iGossiper % lenSide
    val iLeft: Int = if (iCol == 0) -1 else iGossiper - 1
    val iRight: Int = if (iCol == lenSide - 1) nGossipers else iGossiper + 1
    val iUp: Int = iGossiper - iRow
    val iDown: Int = iGossiper + iRow
    Array(iLeft, iRight, iUp, iDown)
  }

  override def neighbors(iGossiper: Int): Array[ActorRef] = {
    iNeighbors(iGossiper).filterNot(i => i < 0 || i >= nGossipers).map(gossipers(_))
  }
}

class ImperfectGridTopology(nGossipers: Int, createGossiper: () => Gossiper) extends GridTopology(nGossipers, createGossiper) {

  val random = new Random(System.currentTimeMillis())

  override def neighbors(iGossiper: Int): Array[ActorRef] = {
    val iGridNeighbors = iNeighbors(iGossiper)
    val iRandNeighbors = Array.range(0, nGossipers).filterNot(i => iGridNeighbors.contains(i) || i == iGossiper)
    val iRandNeighbor = iRandNeighbors(random.nextInt(iRandNeighbors.length))
    val iImperfectGridNeighbors = iGridNeighbors :+ iRandNeighbor
    iImperfectGridNeighbors.map(gossipers(_))
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

  def create(tType: String): (Int, String) => Topology = {
    tType match {
      case "full" =>
        (nGossipers: Int, gType: String) => new FullTopology(nGossipers, GossiperFactory.create(gType))
      case "line" =>
        (nGossipers: Int, gType: String) => new LineTopology(nGossipers, GossiperFactory.create(gType))
      case "grid" =>
        (nGossipers: Int, gType: String) => new GridTopology(roundSquare(nGossipers), GossiperFactory.create(gType))
      case "ipgrid" =>
        (nGossipers: Int, gType: String) => new ImperfectGridTopology(roundSquare(nGossipers), GossiperFactory.create(gType))
    }
  }
}

