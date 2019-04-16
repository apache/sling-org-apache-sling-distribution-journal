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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

public class TopologyViewManagerTest {

    private static final String SUB_AGENT_ID_1 = "subAgentId1";

    private static final String SUB_AGENT_ID_2 = "subAgentId2";

    private static final String AGENT_NAME_1 = "agent1";

    private static final String AGENT_NAME_2 = "agent2";

    private static final long TIMESTAMP_1 = 100;

    private static final long TIMESTAMP_2 = TIMESTAMP_1 + 50;

    private static final long TIMESTAMP_3 = TIMESTAMP_2 + 50;

    private static final long TIMESTAMP_4 = TIMESTAMP_3 + 50;

    private static final State AGENT_NAME_1_STATE_1 = new State(AGENT_NAME_1, SUB_AGENT_ID_1, TIMESTAMP_1, 10L, 0, -1, false);

    private static final State AGENT_NAME_1_STATE_2 = new State(AGENT_NAME_1, SUB_AGENT_ID_2, TIMESTAMP_2, 5L, 0, -1, false);

    private static final State AGENT_NAME_1_STATE_3 = new State(AGENT_NAME_1, SUB_AGENT_ID_1, TIMESTAMP_3, 20L, 0, -1, false);

    private static final State AGENT_NAME_1_STATE_4 = new State(AGENT_NAME_1, SUB_AGENT_ID_1, TIMESTAMP_4, 20L, 0, -1, false);

    private static final State AGENT_NAME_2_STATE_1 = new State(AGENT_NAME_2, SUB_AGENT_ID_1, TIMESTAMP_1, 15L, 0, -1, false);

    private static final State AGENT_NAME_2_STATE_2 = new State(AGENT_NAME_2, SUB_AGENT_ID_2, TIMESTAMP_3, 15L, 0, -1, false);

    private static final long REFRESH_TTL = 10L;

    @Test
    public void testUpdateView() throws Exception {

        TopologyViewManager viewManager = new TopologyViewManager(REFRESH_TTL);

        assertTrue(viewManager.getCurrentView().getSubscribedAgentIds().isEmpty());

        viewManager.refreshState(AGENT_NAME_1_STATE_1);
        viewManager.refreshState(AGENT_NAME_2_STATE_1);
        viewManager.refreshState(AGENT_NAME_1_STATE_2);

        assertTrue(viewManager.getCurrentView().getSubscribedAgentIds().isEmpty());

        viewManager.updateView(0);
        Set<String> s1 = viewManager.getCurrentView().getSubscribedAgentIds();
        assertThat(s1, containsInAnyOrder(SUB_AGENT_ID_1, SUB_AGENT_ID_2));
        assertEquals(3, viewManager.size());

        viewManager.updateView(TIMESTAMP_1);
        Set<String> s2 = viewManager.getCurrentView().getSubscribedAgentIds();
        assertThat(s2, containsInAnyOrder(SUB_AGENT_ID_1, SUB_AGENT_ID_2));
        assertEquals(3, viewManager.size());

        viewManager.updateView(TIMESTAMP_1 + REFRESH_TTL + 1);
        Set<String> s3 = viewManager.getCurrentView().getSubscribedAgentIds();
        assertThat(s3, containsInAnyOrder(SUB_AGENT_ID_2));
        assertEquals(3, viewManager.size());

        viewManager.updateView(TIMESTAMP_2 + REFRESH_TTL + 1);
        Set<String> s4 = viewManager.getCurrentView().getSubscribedAgentIds();
        assertEquals(0, s4.size());
        assertEquals(3, viewManager.size());

        viewManager.refreshState(AGENT_NAME_2_STATE_2);
        viewManager.updateView(TIMESTAMP_2 + REFRESH_TTL + 2);
        Set<String> s5 = viewManager.getCurrentView().getSubscribedAgentIds();
        assertThat(s5, containsInAnyOrder(SUB_AGENT_ID_2));
        assertEquals(4, viewManager.size());

    }

    @Test
    public void testUpdateState() throws Exception {
        TopologyViewManager viewManager = new TopologyViewManager(REFRESH_TTL);

        viewManager.refreshState(AGENT_NAME_1_STATE_1);
        viewManager.updateView(AGENT_NAME_1_STATE_1.getTimestamp());

        State state1 = viewManager.getCurrentView().getState(SUB_AGENT_ID_1, AGENT_NAME_1);
        assertNotNull(state1);
        assertEquals(AGENT_NAME_1_STATE_1.getTimestamp(), state1.getTimestamp());
        assertEquals(AGENT_NAME_1_STATE_1, state1);
        assertEquals(1, viewManager.size());

        viewManager.refreshState(AGENT_NAME_1_STATE_3);
        viewManager.updateView(AGENT_NAME_1_STATE_3.getTimestamp());

        State state2 = viewManager.getCurrentView().getState(SUB_AGENT_ID_1, AGENT_NAME_1);
        assertNotNull(state2);
        assertEquals(AGENT_NAME_1_STATE_3.getTimestamp(), state2.getTimestamp());
        assertEquals(AGENT_NAME_1_STATE_3, state2);
        assertEquals(1, viewManager.size());

        viewManager.refreshState(AGENT_NAME_1_STATE_4);
        viewManager.updateView(AGENT_NAME_1_STATE_4.getTimestamp());

        State state3 = viewManager.getCurrentView().getState(SUB_AGENT_ID_1, AGENT_NAME_1);
        assertNotNull(state3);
        assertEquals(AGENT_NAME_1_STATE_4.getTimestamp(), state3.getTimestamp());
        assertEquals(AGENT_NAME_1_STATE_4, state3);
        assertEquals(1, viewManager.size());
    }
}