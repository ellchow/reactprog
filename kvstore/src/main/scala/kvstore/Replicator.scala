package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.{ Cancellable, ReceiveTimeout }
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case msg@Replicate(key, valueOption, id) =>
      println(msg)
      val seq = nextSeq
      acks = acks + (seq -> (sender, msg))
      context.actorOf(ReplicationRetrier.props(self, replica, Snapshot(key, valueOption, seq)))

    case msg@Replicated(key, seq) =>
      acks.get(seq).foreach{ case (primary, r) =>
        primary ! Replicated(key, r.id)
      }
  }

}

object ReplicationRetrier {
  def props(requestor: ActorRef, replica: ActorRef, ss: Replicator.Snapshot) = Props(classOf[ReplicationRetrier], requestor, replica, ss)
}

class ReplicationRetrier(requestor: ActorRef, replica: ActorRef, ss: Replicator.Snapshot) extends Actor {
  import Replicator._
  import context.dispatcher

  // context.setReceiveTimeout(500 milliseconds)

  val cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, replica, ss)

  def receive: Receive = {
    case SnapshotAck(key, seq) => requestor ! Replicated(key, seq)
    case ReceiveTimeout => // send failure to requestor
  }

}
