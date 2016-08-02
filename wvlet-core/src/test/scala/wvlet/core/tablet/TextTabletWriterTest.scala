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
package wvlet.core.tablet

import wvlet.test.WvletSpec

object TextTabletWriterTest {
  case class Person(id:Int, name:String, phone:Seq[String])
}


import TextTabletWriterTest._
/**
  *
  */
class TextTabletWriterTest extends WvletSpec {

  import wvlet.core._
  import Wvlet._

  "TextTabletWriter" should {

    val seq = Seq(Person(1, "leo", Seq("xxx-xxxx")), Person(2, "yui", Seq("yyy-yyyy", "zzz-zzzz")))

    "output object in JSON array format" in {
      val w  = create(seq) | toJSON[Person]
      w.stream(info(_))
    }

    "output object in CSV format" in {
      val w  = create(seq) | toCSV[Person]
      w.stream(info(_))
    }

    "output object in TSV format" in {
      val w  = create(seq) | toTSV[Person]
      w.stream(info(_))
    }

  }
}
