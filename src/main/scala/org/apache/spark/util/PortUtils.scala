/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.util

import org.slf4j.Logger
import _root_.io.netty.channel.unix.Errors.NativeIoException
import org.sparkproject.jetty.util.MultiException

import java.net.BindException
import scala.jdk.CollectionConverters._

/**
 * Utility-Methods for finding out the next available port to bind to.
 *
 * Derived from org.apache.spark.util.Utils#startServiceOnPort and surrounding methods.
 */
object PortUtils {

  /**
   * Returns the user port to try based upon base and offset.
   * Privileged ports >= 65536 are skipped by rolling over to 1024.
   */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  /**
   * Attempts to start a server on the given port, or fails after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt.
   *
   * @param serverName  Name of the service.
   * @param startServer Function to start server on a given port.
   *                    This is expected to throw java.net.BindException on port collision.
   * @param startPort   The initial port to start the server on.
   * @param maxRetries  : If startPort is already in use,  we will increment port by one and try with that new port.
   *                    maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
   * @return The port on which the Server was started
   */
  def startOnPort(startServer: Int => Int,
                  serverName: String,
                  startPort: Int, maxRetries: Int, logger: Logger ): Int = {

    require(1024 <= startPort && startPort < 65536, "startPort should be between 1024 and 65535 (inclusive)")
    require(maxRetries >= 0, "maxRetries has to be >= 0")

    for (offset <- 0 to maxRetries + 1) {
      val tryPort = userPort(startPort, offset)
      try {
        val port = startServer(tryPort)
        logger.info(s"Successfully started server $serverName on port $port.")
        return port
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Server $serverName failed after $maxRetries retries (starting from $startPort)!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logger.warn(s"Server $serverName could not bind on port $tryPort. Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new IllegalStateException(s"Failed to start server $serverName on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }
}
