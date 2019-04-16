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
package org.apache.sling.distribution.journal.impl.publisher;

import java.util.Objects;
import java.util.UUID;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class AgentId {

    private static final int SLING_ID_LENGTH = UUID.randomUUID().toString().length();

    private final String agentId;

    private final String agentName;

    private final String slingId;

    public AgentId(String agentId) {
        this.agentId = Objects.requireNonNull(agentId);
        slingId = checkSlingId(slingId(agentId));
        agentName = agentName(agentId);
    }

    public AgentId(String slingId, String agentName) {
        this.slingId = checkSlingId(Objects.requireNonNull(slingId));
        this.agentName = Objects.requireNonNull(agentName);
        agentId = agentId(slingId, agentName);
    }

    public String getAgentId() {
        return agentId;
    }

    public String getAgentName() {
        return agentName;
    }

    public String getSlingId() {
        return slingId;
    }

    private String agentId(String slingId, String agentName) {
        return String.format("%s-%s", slingId, agentName);
    }

    private String agentName(String agentId) {
        return agentId.substring(SLING_ID_LENGTH + 1);
    }

    private String slingId(String agentId) {
        return agentId.substring(0, SLING_ID_LENGTH);
    }

    private String checkSlingId(String slingId) {
        if (slingId.length() != SLING_ID_LENGTH) {
            throw new IllegalArgumentException(String.format("Illegal slingId %s", slingId));
        }
        return slingId;
    }

}
