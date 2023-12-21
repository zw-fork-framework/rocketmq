/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.remoting.protocol;

public class RemotingSysResponseCode {

    public static final int SUCCESS = 0;

    public static final int SYSTEM_ERROR = 1;

    /**
     * 1。 broker定时任务处理：BrokerFastFailure
     *      1） Page Cache 繁忙
     *      2） Producer消息在Broker中发生积压
     * 2. broker收到客户端请求
     *      1) 收到Producer请求时，发现Page Cache繁忙 或 transientStorePoolEnable开启时，无可用ByteBuffer
     *      2） broker通过线程池处理客户端请求时，发生异常。如果不是单向请求（Oneway），向Producer返回SYSTEM_BUSY异常
     */
    public static final int SYSTEM_BUSY = 2;

    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;

    public static final int TRANSACTION_FAILED = 4;
}
