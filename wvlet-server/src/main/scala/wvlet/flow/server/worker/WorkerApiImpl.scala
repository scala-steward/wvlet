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
package wvlet.flow.server.worker

import wvlet.flow.api.internal.worker.WorkerApi
import wvlet.flow.api.internal.worker.WorkerApi.TaskExecutionInfo
import wvlet.flow.api.v1.TaskApi._
import wvlet.flow.api.v1.TaskStatus
import wvlet.flow.server.ServerModule.CoordinatorClient
import wvlet.flow.server.worker.WorkerService.WorkerSelf
import wvlet.log.LogSupport

import java.time.Instant

/**
  */
class WorkerApiImpl(node: WorkerSelf, coordinatorClient: CoordinatorClient) extends WorkerApi with LogSupport {
  override def runTask(taskId: TaskId, task: TaskRequest): WorkerApi.TaskExecutionInfo = {
    info(s"Run task: ${taskId}, ${task}")
    coordinatorClient.CoordinatorApi.setTaskStatus(taskId, TaskStatus.RUNNING)
    TaskExecutionInfo(taskId, nodeId = node.name, startedAt = Instant.now())
  }
  override def getTask(taskId: TaskId): Option[TaskRef]      = ???
  override def cancelTask(taskId: TaskId): Option[TaskRef]   = ???
  override def listTasks(request: TaskListRequest): TaskList = ???
}
