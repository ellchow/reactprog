/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging{
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation =>
      root ! op

    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>
      pendingQueue = pendingQueue :+ op

    case CopyFinished =>
      root ! PoisonPill
      root = newRoot

      pendingQueue.foreach(newRoot ! _)
      pendingQueue = Queue.empty

      context.become(normal)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging{
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, e) =>
      if(e == elem){
        removed = false
        requester ! OperationFinished(id)
      }else{
        val pos = if(e < elem) Left else Right
        val child = subtrees.get(pos)

        child.map(_ ! Insert(requester, id, e))
          .getOrElse({
            subtrees = subtrees + (pos -> context.actorOf(BinaryTreeNode.props(e, false), name = "node_"+e))
            requester ! OperationFinished(id)
          })
      }

    case Contains(requester, id, e) =>
      if(e == elem){
        requester ! ContainsResult(id, !removed)
      }else{
        val pos = if(e < elem) Left else Right
        val child = subtrees.get(pos)

        child.map(_ ! Contains(requester, id, e))
          .getOrElse(requester ! ContainsResult(id, false))
      }

    case Remove(requester, id, e) =>
      if(e == elem){
        removed = true
        requester ! OperationFinished(id)
      }else{
        val pos = if(e < elem) Left else Right
        val child = subtrees.get(pos)

        child.map(_ ! Remove(requester, id, e))
          .getOrElse(requester ! OperationFinished(id))
      }

    case CopyTo(treeNode) =>
      val children = subtrees.values.toSet

      if(removed && children.isEmpty){
        context.parent ! CopyFinished
        self ! PoisonPill
      }else{
        if(!removed){
          treeNode ! Insert(self, 0, elem)
        }

        children.foreach{ c =>
          c ! CopyTo(treeNode)
        }

        context.become(copying(children, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished =>
      val remaining = expected - sender

      if(remaining.isEmpty && insertConfirmed){
        context.parent ! CopyFinished
        self ! PoisonPill
      }else{
        context.become(copying(remaining, insertConfirmed))
      }

    case OperationFinished(_) =>
      if(expected.isEmpty){
        context.parent ! CopyFinished
        self ! PoisonPill
      }else{
        context.become(copying(expected, true))
      }
  }

}
