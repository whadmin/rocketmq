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

package org.apache.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.rocketmq.common.MixAll;

/**
 * BrokerData 用来描述一个Broker节点数据。一个Broker节点包含master和一个或多个Slave.
 *
 * 1 在mq中每一个Broker节点的都是相对独立的.他们是不会相互通信的.
 * 2 在mq中每一个Broker节点必须从属于一个集群.反之一个集群有一个或多个BrokerData.
 * 3 在mq中每一个Broker节点内保存着master和Slave物理地址.
 * 4 在mq中每一个Broker节点会告知namerSrv 存储哪些topic消息。因此当我们对一个集群message消息做分片处理只需要将一个topic分配给唯一一个BrokerData即可.
 *
 */
public class BrokerData implements Comparable<BrokerData> {
    //所属集群
    private String cluster;
    //名称（Broker下可以分为master和slave 有相互关系的master Broker和slave Broker名称相同）
    private String brokerName;

    /** brokerId=0代表master  brokerId>0代表slave 参照 MixAll.MASTER_ID*
     *  同一个brokerName下可以有一个Master和多个Slave，
     *  brokerAddrs是一个集合 表示同一个brokerName 下Master和多个Slave的物理地址
     **/
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;

    //在选择一个broker进行连接的时候起作用
    private final Random random = new Random();

    public BrokerData() {

    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    /**
     * 选择一个broker地址，有master就选master的地址
     * 否则随机选一个slave的地址
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String addr = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (addr == null) {
            List<String> addrs = new ArrayList<String>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return addr;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
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
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null)
                return false;
        } else if (!brokerAddrs.equals(other.brokerAddrs))
            return false;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + "]";
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
