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

import static java.lang.Math.abs;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;


public class TopologyViewTest {

    private static final Random RAND = new Random();

    private static final Set<State> STATES = new HashSet<>();

    private static final String SUB_AGENT_ID_1 = "subAgentId1";

    private static final String SUB_AGENT_ID_2 = "subAgentId2";

    private static final String AGENT_NAME_1 = "agent1";

    private static final String AGENT_NAME_2 = "agent2";

    static {
        STATES.add(new State(AGENT_NAME_1, SUB_AGENT_ID_1, 0, 0, 0, -1, false));
        STATES.add(new State(AGENT_NAME_2, SUB_AGENT_ID_1, 0, 0, 0, -1, false));
        STATES.add(new State( AGENT_NAME_1, SUB_AGENT_ID_2, 0, 0, 0, -1, false));
    }

    @Test
    public void testGetSubscribedAgentIdsEmptyView() throws Exception {
        TopologyView emptyView = new TopologyView();
        Set<String> subAgentIds = emptyView.getSubscribedAgentIds("any");
        assertNotNull(subAgentIds);
        assertTrue(subAgentIds.isEmpty());
    }

    @Test
    public void testGetSubscribedAgentIdsFullView() throws Exception {

        TopologyView view = new TopologyView(STATES);

        assertNotNull(view.getSubscribedAgentIds("doesNotExist"));
        assertTrue(view.getSubscribedAgentIds("doesNotExist").isEmpty());

        Set<String> subAgentIdsP1 = view.getSubscribedAgentIds(AGENT_NAME_1);
        assertNotNull(subAgentIdsP1);
        assertEquals(2, subAgentIdsP1.size());
        assertTrue(subAgentIdsP1.contains(SUB_AGENT_ID_1));
        assertTrue(subAgentIdsP1.contains(SUB_AGENT_ID_2));

        Set<String> subAgentIdsP2 = view.getSubscribedAgentIds(AGENT_NAME_2);
        assertNotNull(subAgentIdsP2);
        assertEquals(1, subAgentIdsP2.size());
        assertTrue(subAgentIdsP2.contains(SUB_AGENT_ID_1));

    }

    @Test
    public void testGetSubscriberAgentStatesEmptyView() throws Exception {
        TopologyView emptyView = new TopologyView();
        assertTrue(emptyView.getSubscriberAgentStates("any").isEmpty());
    }

    @Test
    public void testGetSubscriberAgentStatesFullView() throws Exception {

        TopologyView view = new TopologyView(STATES);

        assertTrue(view.getSubscriberAgentStates("doesNotExist").isEmpty());

        Set<State> subStates1 = view.getSubscriberAgentStates(SUB_AGENT_ID_1);
        Set<String> agents = subStates1.stream().map(State::getPubAgentName).collect(Collectors.toSet());
        assertThat(agents, containsInAnyOrder(AGENT_NAME_1, AGENT_NAME_2));

        Set<State> subStates2 = view.getSubscriberAgentStates(SUB_AGENT_ID_2);
        Set<String> agents2 = subStates2.stream().map(State::getPubAgentName).collect(Collectors.toSet());
        assertThat(agents2, containsInAnyOrder(AGENT_NAME_1));

    }
    
    @Test
    public void testMinOffsetByPubAgentName() throws Exception {
        Set<State> states = new HashSet<>(asList(
                new State(AGENT_NAME_1, SUB_AGENT_ID_1, 0, 1, 0, -1, false),
                new State(AGENT_NAME_1, SUB_AGENT_ID_2, 0, 4, 0, -1, false),
                new State(AGENT_NAME_2, SUB_AGENT_ID_1, 0, 5, 0, -1, false)
                ));
        TopologyView view = new TopologyView(states);
        Map<String, Long> offsets = view.getMinOffsetByPubAgentName();
        assertThat(offsets.get(AGENT_NAME_1), equalTo(1L));
        assertThat(offsets.get(AGENT_NAME_2), equalTo(5L));
    }

    @Test
    public void testEquality() {

        assertEquals(new TopologyView(), new TopologyView());
        assertEquals(new TopologyView(), new TopologyView(Collections.emptySet()));

        assertEquals(
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false)),
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false))
        );

        assertEquals(
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false), new State("pub1", "sub2", abs(RAND.nextLong()), 11, 12, -1, false)),
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false), new State("pub1", "sub2", abs(RAND.nextLong()), 11, 12, -1, false))
        );

        assertEquals(
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false), new State("pub2", "sub1", abs(RAND.nextLong()), 21, 22, -1, false)),
                buildView(new State("pub1", "sub1", abs(RAND.nextLong()), 1, 2, -1, false), new State("pub2", "sub1", abs(RAND.nextLong()), 21, 22, -1, false))
        );
    }

    @Test
    public void testInequality() {

        int rand = abs(RAND.nextInt());

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false)),
                buildView(new State("pub1", "sub2", 0, 0, 0, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false)),
                buildView()
        );

        assertNotEquals(
                buildView(),
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false)),
                buildView(new State("pub2", "sub1", 0, 0, 0, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false)),
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false), new State("pub2", "sub1", 0, 0, 0, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false), new State("pub2", "sub1", 0, 0, 0, -1, false)),
                buildView(new State("pub1", "sub1", 0, 0, 0, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, 0, rand, -1, false)),
                buildView(new State("pub1", "sub1", 0, 0, rand + 1, -1, false))
        );

        assertNotEquals(
                buildView(new State("pub1", "sub1", 0, rand, 0, -1, false)),
                buildView(new State("pub1", "sub1", 0, rand + 1, 0, -1, false))
        );

    }

    @Test
    public void testToString() {
        TopologyView view = new TopologyView(Collections.singleton(new State(AGENT_NAME_1, SUB_AGENT_ID_1, 1, 2, 3, -1, false)));
        assertEquals("{\"states\":[{\"timestamp\":1,\"offset\":2,\"retries\":3,\"maxRetries\":-1,\"editable\":false,\"pubAgentName\":\"agent1\",\"subAgentId\":\"subAgentId1\"}]}", view.toString());
    }

    private TopologyView buildView(State ... state) {
        return new TopologyView(new HashSet<>(asList(state)));
    }
}