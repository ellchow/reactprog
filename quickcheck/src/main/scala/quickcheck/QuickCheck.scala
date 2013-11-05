package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("") = forAll{  (as: List[A]) =>
    val h = insertAll(empty, as: _*)
    val decr = readAllDecreasing(h)

    decr == as.sorted.reverse
  }

  def insertAll(h: H, as: A*): H =
    as.foldLeft(h)((hh, a) => insert(a, hh))

  @annotation.tailrec
  final def readAllDecreasing(h: H, as: List[A] = Nil): List[A] =
    if(isEmpty(h)) as else readAllDecreasing(deleteMin(h), findMin(h) :: as)

  lazy val genHeap: Gen[H] = for{
    len <- arbitrary[A].suchThat(i => i >= 0 && i < 100)
    i <- arbitrary[A]
    h <- oneOf(value(empty), genHeap)
  } yield insert(i, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
