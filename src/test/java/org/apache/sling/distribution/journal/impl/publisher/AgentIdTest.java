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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AgentIdTest {

    private static final String SLING_ID = "09b11889-2b8c-4fc1-84e7-e921dc92dbac";

    private static final String SUB_AGENT_NAME_1 = "subAgent1";
    private static final String SUB_AGENT_NAME_2 = "sub_Agent1";
    private static final String SUB_AGENT_NAME_3 = "-sub_Agent1_";
    private static final String SUB_AGENT_NAME_4 = "-sub__Agent1_";
    private static final String SUB_AGENT_NAME_5 = "sub_Ag___ent1";

    private static final String SUB_AGENT_ID_1 = SLING_ID + "-" + SUB_AGENT_NAME_1;
    private static final String SUB_AGENT_ID_2 = SLING_ID + "-" + SUB_AGENT_NAME_2;
    private static final String SUB_AGENT_ID_3 = SLING_ID + "-" + SUB_AGENT_NAME_3;
    private static final String SUB_AGENT_ID_4 = SLING_ID + "-" + SUB_AGENT_NAME_4;
    private static final String SUB_AGENT_ID_5 = SLING_ID + "-" + SUB_AGENT_NAME_5;


    @Test
    public void testGetAgentId() throws Exception {
        assertEquals(SUB_AGENT_ID_1, new AgentId(SLING_ID, SUB_AGENT_NAME_1).getAgentId());
        assertEquals(SUB_AGENT_ID_2, new AgentId(SLING_ID, SUB_AGENT_NAME_2).getAgentId());
        assertEquals(SUB_AGENT_ID_3, new AgentId(SLING_ID, SUB_AGENT_NAME_3).getAgentId());
        assertEquals(SUB_AGENT_ID_4, new AgentId(SLING_ID, SUB_AGENT_NAME_4).getAgentId());
        assertEquals(SUB_AGENT_ID_5, new AgentId(SLING_ID, SUB_AGENT_NAME_5).getAgentId());
    }

    @Test
    public void testGetAgentName() throws Exception {
        assertEquals(SUB_AGENT_NAME_1, new AgentId(SUB_AGENT_ID_1).getAgentName());
        assertEquals(SUB_AGENT_NAME_2, new AgentId(SUB_AGENT_ID_2).getAgentName());
        assertEquals(SUB_AGENT_NAME_3, new AgentId(SUB_AGENT_ID_3).getAgentName());
        assertEquals(SUB_AGENT_NAME_4, new AgentId(SUB_AGENT_ID_4).getAgentName());
        assertEquals(SUB_AGENT_NAME_5, new AgentId(SUB_AGENT_ID_5).getAgentName());
    }

    @Test
    public void testGetSlingId() throws Exception {
        assertEquals(SLING_ID, new AgentId(SUB_AGENT_ID_1).getSlingId());
        assertEquals(SLING_ID, new AgentId(SUB_AGENT_ID_2).getSlingId());
        assertEquals(SLING_ID, new AgentId(SUB_AGENT_ID_3).getSlingId());
        assertEquals(SLING_ID, new AgentId(SUB_AGENT_ID_4).getSlingId());
        assertEquals(SLING_ID, new AgentId(SUB_AGENT_ID_5).getSlingId());
    }

    @Test
    public void testLoop() throws Exception {
        AgentId agentId1 = new AgentId(SLING_ID, SUB_AGENT_ID_1);
        AgentId agentId2 = new AgentId(agentId1.getAgentId());
        assertEquals(agentId1.getAgentId(), agentId2.getAgentId());
        assertEquals(agentId1.getSlingId(), agentId2.getSlingId());
        assertEquals(agentId1.getAgentName(), agentId2.getAgentName());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testIllegal() throws Exception {
        new AgentId(SLING_ID + "1", SUB_AGENT_ID_1);
    }
}