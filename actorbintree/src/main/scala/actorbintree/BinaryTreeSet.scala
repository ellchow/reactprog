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
      val todo = (pendingQueue :+ op)
      pendingQueue = Queue.empty

      todo.foreach(t => root ! t)

    case GC =>
      root ! GC
      context.become(garbageCollecting(createRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>
      pendingQueue = pendingQueue :+ op
    // ignore GC messages
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
      // log.info("insert {} ({}); elem = {}", e, id, elem)
      if(e == elem){
        removed = false
        requester ! OperationFinished(id)
      }else{
        val pos = if(e < elem) Left else Right
        val child = subtrees.get(pos)

        child.map(_ ! Insert(requester, id, e))
          .getOrElse({
            // log.info("added {} to {} of {}", e, pos, elem)
            subtrees = subtrees + (pos -> context.actorOf(BinaryTreeNode.props(e, false)))
            requester ! OperationFinished(id)
          })
      }

    case Contains(requester, id, e) =>
      // log.info("contains {} ({}); elem = {}", e, id, elem)
      if(e == elem){
        // log.info("contains {} found", e)
        requester ! ContainsResult(id, !removed)
      }else{
        val pos = if(e < elem) Left else Right
        val child = subtrees.get(pos)

        // log.info("contains {} recursing to {} of {}", e, pos, elem)

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

    case opr: OperationReply => context.parent ! opr
  }



  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???

}
