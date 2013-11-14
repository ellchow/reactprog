package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run

    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run

    assert(out.getSignal === true, "and 3")
  }


  test("orGate example") {
    val in1, in2, out = new Wire
    orGate(in1, in2, out)

    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false, "or 1")

    in1.setSignal(true)
    run

    assert(out.getSignal === true, "or 2")

    in2.setSignal(true)
    run

    assert(out.getSignal === true, "or 3")

    in1.setSignal(false)
    run

    assert(out.getSignal === true, "or 4")
  }

  test("demux example") {
    def makeInputsAndOutputs(n: Int) =
      (List.fill(n)(new Wire), List.fill(math.pow(2,n).toInt)(new Wire))

    val in = new Wire
    val (c, out) = makeInputsAndOutputs(0)
    demux(in, c, out)

    in.setSignal(false)
    run

    assert(out.head.getSignal === false, "demux base 1")

    in.setSignal(true)
    run

    assert(out.head.getSignal === true, "demux base 1")

    def intBitsToBooleans(x: Int, n: Int): List[Boolean] = if(n > 0){
      ((x & (1 << (n - 1))) != 0) :: intBitsToBooleans(x, n - 1)
    }else{
      Nil
    }

    for{
      n <- 0 to 10
      i <- 0 until math.pow(2,n).toInt
    }{
      val in = new Wire
      val (c, out) = makeInputsAndOutputs(n)
      demux(in, c, out)
      intBitsToBooleans(i, n).zip(c).foreach{
        case (sig, cc) => cc.setSignal(sig)
      }
      in.setSignal(true)
      run

      assert(out.map(_.getSignal).reverse.indexOf(true) === i, s"demux index (n=$n, i=$i)")
      assert(out.map(_.getSignal).filter(identity).size === 1, s"demux sum (n=$n, i=$i)")
    }
  }

}
