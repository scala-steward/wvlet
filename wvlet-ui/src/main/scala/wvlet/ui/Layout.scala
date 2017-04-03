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
package wvlet.ui
import org.scalajs.dom
import org.scalajs.dom.document
import org.scalajs.dom.ext.KeyCode

import scalatags.JsDom.all._

/**
  *
  */
object Layout {

  def icon(iconName:String) = {
    i(cls := "material-icons", role := "presentation")(iconName)
  }

  def searchBox = {
    val box = input(cls:="mdl-textfield__input", tpe:="text", id:="search-box").render
    box.onkeypress = (e:dom.KeyboardEvent) => {
      if(e.keyCode == KeyCode.Enter) {
        val b = div(cls:="alert alert-success", attr("role"):="alert")(
          s"Searching for ${box.value}"
        ).render

        val status = document.getElementById("status")
        while(status.hasChildNodes()) {
          status.removeChild(status.firstChild)
        }
        status.appendChild(b)
      }
    }

    val searchForm =
      form(action:="#")(
        div(cls:="mdl-textfield mdl-js-textfield") (
          div(cls:="mdl-textfield")(
            box,
            label(cls:="mdl-textfield__label", attr("for"):="search-box") ("Search ...")
          )
        )
      )

    searchForm
  }

  def navLink(url:String, name:String, iconName:String) = {
    li(cls:="nav-item")(
      a(cls := "nav-link", href := url)(
        icon(iconName),
        name
      )
    )
  }

  def dataTable(tableHeader:Seq[String], rows:Seq[Seq[Any]]) = {
    table(cls:="table table-sm")(
      thead(cls:="thead-inverse")(
        tr(
          for(h <- tableHeader) yield th(cls:="mdl-data-table__cell--non-numeric")(h)
        )
      ),
      tbody(
        for(row <- rows) yield {
          tr(
            for(col <- row) yield {
              td(col.toString)
            }
          )
        }
      )
    )
  }


  def layout(title:String) = {
    div(cls:="mdl-layout mdl-js-layout mdl-layout--fixed-drawer mdl-layout--fixed-header has-drawer")(
//      header(cls:="mdl-layout__header")(
//        div(cls:="mdl-layout__header-row")(
//          searchBox,
//          div(cls:="mdl-layout-spacer")
////          tag("nav")(cls:="mdl-navigation mdl-layout--lage-screen-only")(
////            navLink("", "Home"),
////            navLink("", "Link")
////          )
//        )
//      ),
      div(cls:="container-fluid")(
        div(cls:="row")(
          tag("nav")(cls:="col-sm-3 col-md-2 sidebar")(
            span(cls:="mdl-layout-title")(title),
            ul(cls:="nav nav-pills flex-column")(
              navLink("", "Home", "home"),
              navLink("", "List", "list"),
              navLink("", "Settings", "settings")
            )
          )
        ),
        tag("main")(cls:="col-sm-9 offset-sm-3 col-md-10 offset-md-2 pt-3")(
          div(cls:="container")(
            div(cls := "row")(
              div(id := "status")
            ),
            div(cls := "row") (
              div(id := "main")
            )
          )
        )
      )
    )
  }
}