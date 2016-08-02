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

import wvlet.inject.InjectionException.ErrorType
import wvlet.obj.ObjectType

object InjectionException {

  sealed trait ErrorType {
    def errorCode: String = this.toString
  }
  case class MISSING_CONTEXT(cl:ObjectType) extends ErrorType
  case class CYCLIC_DEPENDENCY(deps: Set[ObjectType]) extends ErrorType

}
/**
  *
  */
class InjectionException(errorType: ErrorType) extends Exception(errorType.toString) {

}


