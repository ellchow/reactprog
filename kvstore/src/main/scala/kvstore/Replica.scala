package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor, ReceiveTimeout, Cancellable }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class CleanupFailedRequest(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import PersistenceRetrier._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps)
  var requests = Map.empty[Long,(ActorRef, Int, Cancellable)]

  override def preStart() { arbiter ! Join }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  def receiveGet: Receive = { case Get(key, id) => sender ! GetResult(key, kv.get(key), id) }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = receiveGet.orElse{
    case Replicas(ss) =>
      val newReplicas = ss -- secondaries.keys
      val newSecondaries = newReplicas.map(r => r -> context.actorOf(Replicator.props(r))).toMap

      replicators = replicators ++ newSecondaries.values
      secondaries = secondaries ++ newSecondaries

    case msg@Insert(key, value, id) =>
      println(msg)
      val cancellable = context.system.scheduler.scheduleOnce(1 second, self, CleanupFailedRequest(id))
      requests = requests + (id -> (sender, 0, cancellable))
      kv = kv + (key -> value)
      replicators.foreach(_ ! Replicate(key, Some(value), id))
      context.actorOf(PersistenceRetrier.props(self, persistence, Persist(key, Some(value), id), Replicated.apply _))


    case msg@Remove(key, id) =>
      println(msg)
      val cancellable = context.system.scheduler.scheduleOnce(1 second, self, CleanupFailedRequest(id))
      requests = requests + (id -> (sender, 0, cancellable))
      kv = kv - key
      replicators.foreach(_ ! Replicate(key, None, id))
      context.actorOf(PersistenceRetrier.props(self, persistence, Persist(key, None, id), Replicated.apply _))

    case Replicated(key, id) =>
      requests.get(id).foreach{ case (client, acksReceived, cancellable) =>
        if((acksReceived + 1) >= replicators.size){
          cancellable.cancel()
          requests = requests - id
          client ! OperationAck(id)
        }else{
          requests = requests + (id -> (client, acksReceived + 1, cancellable))
        }
      }

    case FailedPersistence(key, id) =>
      requests.get(id).foreach{ case (client, acksReceived, cancellable) =>
        cancellable.cancel()
        requests = requests - id
        client ! OperationFailed(id)
      }

    case CleanupFailedRequest(id) =>
      requests.get(id).foreach{ case (client, acksReceived, cancellable) =>
        requests = requests - id
        client ! OperationFailed(id)
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = receiveGet.orElse{
    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      valueOption match {
        case Some(value) => kv = kv.updated(key, value)
        case None => kv = kv - key
      }
      context.actorOf(PersistenceRetrier.props(sender, persistence, Persist(key, valueOption, seq), SnapshotAck.apply _))

      context.become(replica(seq + 1L))

    case Snapshot(_, _, seq) if seq > expectedSeq => Unit

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)

  }

}



object PersistenceRetrier {
  import Persistence._

  case class FailedPersistence(key: String, id: Long)

  def props[A](requestor: ActorRef, persistence: ActorRef, p: Persist, response: (String, Long) => A) = Props(classOf[PersistenceRetrier[A]], requestor, persistence, p, response)
}

class PersistenceRetrier[A](requestor: ActorRef, persistence: ActorRef, p: Persistence.Persist, response: (String, Long) => A) extends Actor {
  import PersistenceRetrier._
  import Persistence._
  import Replicator._
  import context.dispatcher

  context.setReceiveTimeout(1 second)

  val handle = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, p)

  def receive: Receive = {
    case Persisted(key, seq) =>
      handle.cancel()
      requestor ! response(key, seq)
    case ReceiveTimeout => requestor ! FailedPersistence(p.key, p.id)
  }

}
