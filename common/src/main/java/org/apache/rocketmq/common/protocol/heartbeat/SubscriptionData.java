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

/**
 * $Id: SubscriptionData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.HashSet;
import java.util.Set;

/**
 * 用来形容要订阅什么样的消息数据
 */
public class SubscriptionData implements Comparable<SubscriptionData> {
    /**
     * 订阅所有
     */
    public final static String SUB_ALL = "*";
    /**
     * Consumer端过滤相关的，用户自定义过滤类时才会用
     */
    private boolean classFilterMode = false;
    /**
     * topic
     */
    private String topic;
    /**
     * 订阅的标示,如expressionType默认TAG时，这里可以填写"A||B"
     */
    private String subString;
    /**
     * topic下订阅的标签tag
     */
    private Set<String> tagsSet = new HashSet<String>();
    /**
     * 每个tag对应的hashCode
     */
    private Set<Integer> codeSet = new HashSet<Integer>();
    /**
     * 版本,通过时间来实现
     */
    private long subVersion = System.currentTimeMillis();
    /**
     * TAG或SQL92,参照ExpressionType类
     * 默认null也是当成TAG来处理，参照 ExpressionType#isTagType(java.lang.String)
     */
    private String expressionType;

    /**
     * Consumer端过滤相关的，用户自定义过滤类时才会用
     */
    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    public String getFilterClassSource() {
        return filterClassSource;
    }

    public void setFilterClassSource(String filterClassSource) {
        this.filterClassSource = filterClassSource;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubString() {
        return subString;
    }

    public void setSubString(String subString) {
        this.subString = subString;
    }

    public Set<String> getTagsSet() {
        return tagsSet;
    }

    public void setTagsSet(Set<String> tagsSet) {
        this.tagsSet = tagsSet;
    }

    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }

    public Set<Integer> getCodeSet() {
        return codeSet;
    }

    public void setCodeSet(Set<Integer> codeSet) {
        this.codeSet = codeSet;
    }

    public boolean isClassFilterMode() {
        return classFilterMode;
    }

    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode)
            return false;
        if (codeSet == null) {
            if (other.codeSet != null)
                return false;
        } else if (!codeSet.equals(other.codeSet))
            return false;
        if (subString == null) {
            if (other.subString != null)
                return false;
        } else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
            return false;
        if (tagsSet == null) {
            if (other.tagsSet != null)
                return false;
        } else if (!tagsSet.equals(other.tagsSet))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (expressionType == null) {
            if (other.expressionType != null)
                return false;
        } else if (!expressionType.equals(other.expressionType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
            + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
            + ", expressionType=" + expressionType + "]";
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }
}
