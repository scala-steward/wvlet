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
package wvlet.dataflow.server.coordinator

import wvlet.dataflow.api.v1.{TaskApi, TaskList, TaskListRequest, TaskRef, TaskRequest}
import wvlet.dataflow.api.v1.TaskApi.TaskId
import wvlet.log.LogSupport

/**
  */
class TaskApiImpl(taskManager: TaskManager) extends TaskApi with LogSupport {

  override def newTask(request: TaskRequest): TaskRef = {
    taskManager.dispatchTask(request)
  }

  override def getTask(taskId: TaskId): Option[TaskRef] = {
    taskManager.getTaskRef(taskId)
  }

  override def listTasks(taskListRequest: TaskListRequest): TaskList = {
    val tasks = taskManager.getAllTasks
    val selectedTasks = taskListRequest.limit match {
      case Some(limit) => tasks.take(tasks.size.min(limit))
      case None        => tasks
    }
    TaskList(
      tasks = selectedTasks
    )
  }

  override def cancelTask(taskId: TaskId): Option[TaskRef] = ???
}
