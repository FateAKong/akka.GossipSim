/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/29/13
 * Time: 5:35 PM
 */

import akka.actor.{Props, ActorSystem}

object GossipSim extends App {

  if (args.length <3 || args.length>4) {

  } else {
    // parse the params
    val termCnt: Int = if (args.length==4) args(3).toInt else 0
    val nGossipers: Int = args(0).toInt
    val tType: String = args(1)
    val gType: String = args(2)

    val system = ActorSystem("GossipSimSys")
    val topology = system.actorOf(Props(TopologyFactory.create(tType, termCnt)(nGossipers, gType)))
    topology ! Start
  }
}
