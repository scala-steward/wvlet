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
package wvlet.flow.api.v1

import wvlet.airframe.http.RPC
import CoordinatorApi._
import wvlet.airframe.ulid.ULID
import wvlet.flow.api.v1.TaskRef.TaskId

import java.time.Instant

/**
  * Coordinator is a central manager of task execution control. It receives requests for running tasks.
  *
  * A coordinator can have multiple worker nodes for distributed processing.
  */
@RPC
trait CoordinatorApi {
  def newTask(request: TaskRequest): TaskRef
  def getTask(taskId: TaskId): Option[TaskRef]
  def listTasks(taskListRequest: TaskListRequest): TaskList
  def cancelTask(taskId: TaskId): Option[TaskRef]

  def listNodes: Seq[NodeInfo]
  def register(node: Node): Unit
}

object CoordinatorApi {
  type NodeId = String

  case class Node(name: NodeId, address: String, isCoordinator: Boolean, startedAt: Instant)
  case class NodeInfo(node: Node, lastHeartbeatAt: Instant) {
    def isCoordinator: Boolean = node.isCoordinator
  }
}
