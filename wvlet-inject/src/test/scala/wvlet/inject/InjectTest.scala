/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.inject

import java.io.PrintStream
import java.util.concurrent.atomic.AtomicInteger

import wvlet.log.LogSupport
import wvlet.obj.ObjectType
import wvlet.obj.tag.@@
import wvlet.test.WvletSpec

import scala.reflect.ClassTag
import scala.util.Random

case class ExecutorConfig(numThreads: Int)

object ServiceMixinExample {

  trait Printer {
    def print(s: String): Unit
  }

  case class ConsoleConfig(out: PrintStream)

  class ConsolePrinter(config: ConsoleConfig) extends Printer with LogSupport {
    info(s"using config: ${config}")

    def print(s: String) {config.out.println(s)}
  }

  class LogPrinter extends Printer with LogSupport {
    def print(s: String) {info(s)}
  }

  class Fortune {
    def generate: String = {
      val pattern = Seq("Hello", "How are you?")
      pattern(Random.nextInt(pattern.length))
    }
  }

  trait PrinterService {
    protected def printer = inject[Printer]
  }

  trait FortuneService {
    protected def fortune = inject[Fortune]
  }

  /**
    * Mix-in printer/fortune instances
    * Pros:
    *   - trait can be shared with multiple components
    *   - xxxService trait can be a module
    * Cons:
    *   - Need to define XXXService boilerplate, which just has a val or def of the service object
    *   - Cannot change the variable name without defining additional XXXService trait
    *     - Need to care about variable naming conflict
    *   - We don't know the missing dependenncy at compile time
    */
  trait FortunePrinterMixin extends PrinterService with FortuneService {
    printer.print(fortune.generate)
  }

  /**
    * Using local val/def injection
    *
    * Pros:
    *   - Service reference (e.g., printer, fortune) can be scoped inside the trait.
    *   - You can save boilerplate code
    * Cons:
    *   - If you use the same service multiple location, you need to repeat the same description in the user module
    *   - To reuse it in other traits, we still need to care about the naming conflict
    */
  trait FortunePrinterEmbedded {
    protected def printer = inject[Printer]
    protected def fortune = inject[Fortune]

    printer.print(fortune.generate)
  }

  /**
    * Using Constructor for dependency injection (e.g., Guice)
    *
    * Pros:
    *   - Close to the traditional OO programming style
    *   - Can be used without DI framework
    * Cons:
    *   - To add/remove modules, we need to create another constructor or class.
    * -> code duplication occurs
    *   - Enhancing the class functionality
    *   - RememenbOrder of constructor arguments
    * -
    */
  class FortunePrinterAsClass @Inject()(printer: Printer, fortune: Fortune) {
    printer.print(fortune.generate)
  }


  class HeavyObject() extends LogSupport {
    info(f"Heavy Process!!: ${this.hashCode()}%x")
  }

  trait HeavySingletonService {
    val heavy = inject[HeavyObject]
  }

  trait HelixAppA extends HeavySingletonService {
  }

  trait HelixAppB extends HeavySingletonService {
  }

  case class A(b: B)
  case class B(a: A)

  class EagerSingleton extends LogSupport {
    info("initialized")
    val initializedTime = System.nanoTime()
  }

  class ClassWithContext(val c: Session) extends FortunePrinterMixin with LogSupport {
    //info(s"context ${c}") // we should access context since Scala will remove private field, which is never used
  }

  case class HelloConfig(message: String)

  class FactoryExample(val c: Session) {
    val hello  = inject { config: HelloConfig => s"${config.message}" }
    val hello2 = inject { (c1: HelloConfig, c2: EagerSingleton) => s"${c1.message}:${c2.getClass.getSimpleName}" }

    val helloFromProvider = inject(provider _)

    def provider(config: HelloConfig): String = config.message
  }

  case class Fruit(name: String)

  trait Apple
  trait Banana
  trait Lemon

  trait TaggedBinding {
    val apple  = inject[Fruit @@ Apple]
    val banana = inject[Fruit @@ Banana]
    val lemon  = inject(lemonProvider _)

    def lemonProvider(f: Fruit @@ Lemon) = f
  }


  trait Nested {
    val nest = inject[Nest1]
  }

  trait Nest1 {
    val nest2 = inject[Nest2]
  }

  class Nest2()

}

import wvlet.inject.ServiceMixinExample._

/**
  *
  */
class InjectTest extends WvletSpec {

  "Helix" should {

    "instantiate class" in {

      val h = new Inject
      h.bind[Printer].to[ConsolePrinter]
      h.bind[ConsoleConfig].toInstance(ConsoleConfig(System.err))

      val context = h.newContext
      val m = context.build[FortunePrinterMixin]
    }


    "create singleton" in {
      val h = new Inject
      h.bind[HeavyObject].toSingleton

      val c = h.newContext
      val a = c.build[HelixAppA]
      val b = c.build[HelixAppB]
      a.heavy shouldEqual b.heavy
    }

    "create singleton eagerly" in {
      val start = System.nanoTime()
      val h = new Inject
      h.bind[EagerSingleton].toEagerSingleton
      val c = h.newContext
      c.get[HeavyObject]
      val current = System.nanoTime()
      val s = c.get[EagerSingleton]

      s.initializedTime should be > start
      s.initializedTime should be < current
    }


    "found cyclic dependencies" in {
      val c = new Inject().newContext
      trait HasCycle {
        val obj = inject[A]
      }
      warn(s"Running cyclic dependency test: A->B->A")
      intercept[InjectionException] {
        c.build[HasCycle]
      }
    }

    "Find a context in parameter" in {
      val h = new Inject
      h.bind[Printer].to[ConsolePrinter]
      h.bind[ConsoleConfig].toInstance(ConsoleConfig(System.err))
      val c = h.newContext
      new ClassWithContext(c)
    }

    "support injection listener" in {
      val h = new Inject
      h.bind[EagerSingleton].toEagerSingleton
      h.bind[ConsoleConfig].toInstance(ConsoleConfig(System.err))

      val counter = new AtomicInteger(0)
      h.addListner(new SessionListener {
        override def afterInjection(t: ObjectType, injectee: Any): Unit = {
          counter.incrementAndGet()
        }
      })
      val c = h.newContext
      c.get[ConsoleConfig]
      counter.get shouldBe 2
    }

    "support injection via factory" in {
      val h = new Inject
      h.bind[HelloConfig].toInstance(HelloConfig("Hello Helix!"))
      val c = h.newContext
      val f = new FactoryExample(c)
      f.hello shouldBe "Hello Helix!"
      f.helloFromProvider shouldBe "Hello Helix!"

      info(f.hello2)
    }

    "support type tagging" taggedAs ("tag") in {
      val h = new Inject
      h.bind[Fruit @@ Apple].toInstance(Fruit("apple"))
      h.bind[Fruit @@ Banana].toInstance(Fruit("banana"))
      h.bind[Fruit @@ Lemon].toInstance(Fruit("lemon"))
      val c = h.newContext
      val tagged = c.build[TaggedBinding]
      tagged.apple.name shouldBe ("apple")
      tagged.banana.name shouldBe ("banana")
      tagged.lemon.name shouldBe ("lemon")
    }


    "support nested context injection" taggedAs("nested") in {
      val h = new Inject
      val c = h.newContext
      c.build[Nested]
    }

  }
}
