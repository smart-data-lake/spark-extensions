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

import org.scalatest.funsuite.AnyFunSuite

class PortUtilsTest extends AnyFunSuite {

  test("userPort should roll over when base + offset > 65535") {
    assert(PortUtils.userPort(65534, 0) == 65534)
    assert(PortUtils.userPort(65534, 1) == 65535)
    assert(PortUtils.userPort(65534, 2) == 1024)
    assert(PortUtils.userPort(65534, 3) == 1025)
  }
}
