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
package wvlet.ui.component

import org.scalajs.dom
import rx._

import scalatags.JsDom.all._
import Ctx.Owner.Unsafe._

import scala.scalajs.js

/**
  *
  */
object Navbar {

  var currentPage = Var("Home")

  def state = {
    dom.document.URL
  }

  def icon(iconName:String) = {
    i(cls := "material-icons", role := "presentation")(iconName)
  }

  case class NavLink(url:String, name:String, icon:String)
  val links = Seq(
    NavLink("", "Home", "home"),
    NavLink("", "List", "list"),
    NavLink("", "Settings", "settings")
  )


  def renderNav(currentPage:String) = {
    tag("nav")(cls:="col-sm-3 col-md-2 hidden-xs-down sidebar bg-faded")(
      a(cls:="navbar", href:="#")("wvlet"),
      ul(cls:="nav nav-pills flex-column")(
        for (l <- links) yield {
          val isActive = currentPage == l.name
          val linkClass = s"nav-link${if (isActive) " active" else ""}"
          val anchor = a(cls := linkClass, href:=l.url)(
            icon(l.icon),
            l.name
          ).render
          anchor.onclick = (e:dom.MouseEvent) => {
            e.preventDefault()
            Navbar.currentPage() = l.name
          }
          li(cls := "nav-item")(anchor)
        }
      )
    ).render
  }

  def render = {
    var last = renderNav("Home")
    currentPage.foreach{ page =>
      val newLast = renderNav(page)
      if(last.parentNode != null) {
        last.parentNode.replaceChild(newLast, last)
      }
      last = newLast
    }
    last
  }
}

