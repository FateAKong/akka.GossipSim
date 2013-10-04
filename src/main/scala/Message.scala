/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/29/13
 * Time: 6:42 PM
 */


sealed trait Message

case object Start extends Message

case object Ready extends Message

case object Send extends Message

case object Term extends Message

case class Init(id: Int, iNeighbors: Array[Int]) extends Message

case class Result(res: Double) extends Message

case object First extends Message

case class First(s: Double, w: Double) extends Message

case object Content extends Message

case class Content(s: Double, w: Double) extends Message