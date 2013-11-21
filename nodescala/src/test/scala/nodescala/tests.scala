package nodescala



import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be created") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("A Future of list should be created") {
    val lst = (1 to 10).toList
    val all = Future.all(lst.map(i => Future.always(i)))

    val res = Await.result(all, 1 second)
    assert(res == lst)
  }


  test("Any value may be returned in Future") {
    for(_ <- 1 to 20){
      val ok = List(1,2)
      val except = new Exception("die!")
      val any = Future.any(Future(throw except) :: ok.map(x => Future(x)))
      try{
        val res = Await.result(any, 1 second)

        assert(ok.contains(res))
      }catch {
        case e: Exception => assert(e == except)
      }
    }
  }

  test("Delay should pause for specified duration") {
    val s = 1
    val delay = Future.delay(s seconds)

    val start = System.currentTimeMillis
    Await.result(delay, (s + 1) seconds)
    val end = System.currentTimeMillis

    assert(math.abs((end - start) / 1000.0) <= (s + 0.01))
  }

  test("Now should get value if available") {
    try{
      Future.never[Int].now
      assert(false)
    } catch {
      case e: NoSuchElementException => assert(true)
    }

    try{
      val t = Future.always(1).now
      assert(t == 1)
    } catch {
      case e: NoSuchElementException => assert(false)
    }

    val except = new Exception("die")
    try{
      Future.always(throw except).now
      assert(false)
    } catch {
      case e: NoSuchElementException => assert(false)
      case e: Exception => assert(e == except)
    }
  }

  test("continueWith transforms future after completion") {
    val doubled: Future[Int] => Int = Await.result(_, 0 seconds) * 2

    try{
      Await.result(Future.never[Int].continueWith(doubled), 1 second)
      assert(false)
    } catch {
      case e: TimeoutException => assert(true)
    }

    try{
      val t = Await.result(Future.always(1).continueWith(doubled), 1 second)
      assert(t == 2)
    } catch {
      case e: TimeoutException => assert(false)
    }

    val except = new Exception("die")
    try{
      Future.always[Int](throw except).continueWith(doubled)
      assert(false)
    } catch {
      case e: Exception => assert(e == except)
    }
  }

  test("continue transforms result after completion") {
    val doubled: Try[Int] => Int = _ match {
      case Success(i) => 2 * i
      case Failure(e) => throw e
    }

    try{
      Await.result(Future.never[Int].continue(doubled), 1 second)
      assert(false)
    } catch {
      case e: TimeoutException => assert(true)
    }

    try{
      val t = Await.result(Future.always(1).continue(doubled), 1 second)
      assert(t == 2)
    } catch {
      case e: TimeoutException => assert(false)
    }

    val except = new Exception("die")
    try{
      Future.always[Int](throw except).continue(doubled)
      assert(false)
    } catch {
      case e: Exception => assert(e == except)
    }
  }

  /*
  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)

      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }
  */
}




