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
package org.apache.rocketmq.store;

/**
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    PUT_OK,//正常情况
    END_OF_FILE,//当消息(含批量)所需的长度超过文件剩余长度时返回
    MESSAGE_SIZE_EXCEEDED,//当消息(含批量)所需的长度超过了MessageStoreConfig.maxMessageSize(默认4M)时触发，代表单个消息过大
    PROPERTIES_SIZE_EXCEEDED,//当单个消息所需的prop文件长度超过了Short.MAX_VALUE时触发
    UNKNOWN_ERROR,//其他未知错误
}
