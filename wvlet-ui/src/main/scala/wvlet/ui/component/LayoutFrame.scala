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

import mhtml._

/**
  *
  */
object LayoutFrame extends RxComponent {

  def body[T <: scala.xml.Node](body: T) =

    <div class="mdl-layout mdl-js-layout mdl-layout--fixed-drawer
            mdl-layout--fixed-header">
      <header class="mdl-layout__header">
        <div class="mdl-layout__header-row">
          <div class="mdl-layout-spacer"></div>
          {SearchBar.body}
        </div>
      </header>
      {Navbar.body}
      <main class="mdl-layout__content">
        <div class="page-content">

          <div class="mdl-grid">
            {body}
          </div>
        </div>
      </main>
    </div>
}
