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

import wvlet.log.LogSupport
import scala.reflect.{macros => sm}

/**
  *
  */
object InjectMacros extends LogSupport {

  def buildImpl[A: c.WeakTypeTag](c: sm.Context)(ev: c.Tree): c.Expr[A] = {
    import c.universe._
    val t = ev.tpe
    c.Expr(
      q"""new ${ev.tpe.typeArgs(0)} { protected def __current_session = ${c.prefix} }"""
    )
  }

  def injectImpl[A: c.WeakTypeTag](c: sm.Context)(ev: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(
      q"""{
         val c = wvlet.inject.Inject.findSession(this)
         c.get(${ev})
        }
      """
    )
  }

  def inject1Impl[A: c.WeakTypeTag, D1: c.WeakTypeTag](c: sm.Context)(factory: c.Tree)(a: c.Tree, d1: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(
      q"""{
         val c = wvlet.inject.Inject.findSession(this)
         c.getOrElseUpdate($factory(c.get(${d1})))
        }
      """
    )
  }

  def inject2Impl[A: c.WeakTypeTag, D1: c.WeakTypeTag, D2: c.WeakTypeTag]
  (c: sm.Context)(factory: c.Tree)
  (a: c.Tree, d1: c.Tree, d2: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(q"""{ val c = wvlet.inject.Inject.findSession(this); c.getOrElseUpdate($factory(c.get(${d1}), c.get(${d2}))) }""")
  }

  def inject3Impl[A: c.WeakTypeTag, D1: c.WeakTypeTag, D2: c.WeakTypeTag, D3: c.WeakTypeTag]
  (c: sm.Context)(factory: c.Tree)
  (a: c.Tree, d1: c.Tree, d2: c.Tree, d3: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(q"""{ val c = wvlet.inject.Inject.findSession(this); c.getOrElseUpdate($factory(c.get(${d1}), c.get(${d2}), c.get(${d3}))) }""")
  }

  def inject4Impl[A: c.WeakTypeTag, D1: c.WeakTypeTag, D2: c.WeakTypeTag, D3: c.WeakTypeTag, D4: c.WeakTypeTag]
  (c: sm.Context)(factory: c.Tree)
  (a: c.Tree, d1: c.Tree, d2: c.Tree, d3: c.Tree, d4: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(
      q"""{ val c = wvlet.inject.Inject.findSession(this); c.getOrElseUpdate($factory(c.get(${d1}), c.get(${d2}), c.get(${d3}), c.get(${d4}))) }""")
  }

  def inject5Impl[A: c.WeakTypeTag, D1: c.WeakTypeTag, D2: c.WeakTypeTag, D3: c.WeakTypeTag, D4: c.WeakTypeTag, D5: c.WeakTypeTag]
  (c: sm.Context)(factory: c.Tree)
  (a: c.Tree, d1: c.Tree, d2: c.Tree, d3: c.Tree, d4: c.Tree, d5: c.Tree): c.Expr[A] = {
    import c.universe._
    c.Expr(
      q"""{ val c = wvlet.inject.Inject.findSession(this); c.getOrElseUpdate($factory(c.get(${d1}), c.get(${d2}), c.get(${d3}), c.get(${d4}), c.get(${d5}))) }""")
  }

}
