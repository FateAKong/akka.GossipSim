/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 9/29/13
 * Time: 5:35 PM
 */

import akka.actor.{Props, ActorSystem}

object GossipSim extends App {

  if (args.length == 3) {

    val system = ActorSystem("GossipSimSys")
    val topology = system.actorOf(Props(TopologyFactory.create(args(1))(args(0).toInt, args(2))))
    topology ! Start
  }
}
zz