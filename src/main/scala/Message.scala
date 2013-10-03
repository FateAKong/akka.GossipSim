/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/29/13
 * Time: 6:42 PM
 */

import akka.actor.ActorRef

sealed trait Message

case object Start extends Message

case object Spread extends Message

case object Wait extends Message

case object Ready extends Message

case object Done extends Message

case object Content extends Message

case class Content(val s: Double, val w: Double) extends Message

case class Init(_id: Int, _neighbors: Array[ActorRef])

case class Remove(removeMe: ActorRef) extends Message
