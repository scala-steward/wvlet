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
package wvlet.flow.server

import io.grpc.{ConnectivityState, ManagedChannel, ManagedChannelBuilder}
import wvlet.airframe.control.Control
import wvlet.airframe.http.ServerAddress
import wvlet.flow.api.WvletGrpcClient
import wvlet.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec

/**
  */
class RPCClientProvider(workerConfig: WorkerConfig) extends LogSupport with AutoCloseable {

  import scala.jdk.CollectionConverters._

  private val clientHolder = new ConcurrentHashMap[String, WvletGrpcClient.SyncClient]().asScala

  def getSyncClientFor(nodeAddress: ServerAddress): WvletGrpcClient.SyncClient = {
    getSyncClientFor(nodeAddress.hostAndPort)
  }

  def getSyncClientFor(nodeAddress: String): WvletGrpcClient.SyncClient = {
    clientHolder.getOrElseUpdate(
      nodeAddress, {
        val channel: ManagedChannel = ManagedChannelBuilder.forTarget(nodeAddress).usePlaintext().build()

        @tailrec
        def loop: Unit = {
          channel.getState(true) match {
            case ConnectivityState.READY =>
              info(s"Channel for ${nodeAddress} is ready")
            // OK
            case ConnectivityState.SHUTDOWN =>
              throw new IllegalStateException(s"Failed to open a channel for ${nodeAddress}")
            case other =>
              warn(s"Channel state for ${nodeAddress} is ${other}. Sleeping for 100ms")
              Thread.sleep(100)
              loop
          }
        }

        loop
        WvletGrpcClient.newSyncClient(channel)
      }
    )
  }

  override def close(): Unit = {
    Control.closeResources(clientHolder.values.toSeq: _*)
  }
}
