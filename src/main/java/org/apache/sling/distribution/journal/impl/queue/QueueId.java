/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.impl.queue;

public class QueueId {
    private final String pubAgentName;
    private final String subSlingId;
    private final String subAgentName;
    private final String queueName;
    
    public QueueId(String pubAgentName, String subSlingId, String subAgentName, String queueName) {
        this.pubAgentName = pubAgentName;
        this.subSlingId = subSlingId;
        this.subAgentName = subAgentName;
        this.queueName = queueName;
    }
    
    public String getPubAgentName() {
        return pubAgentName;
    }
    
    public String getSubSlingId() {
        return subSlingId;
    }
    
    public String getSubAgentName() {
        return subAgentName;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public String getErrorQueueKey() {
        return String.format("%s#%s#%s", pubAgentName, subSlingId, subAgentName);
    }
}
