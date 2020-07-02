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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Random;

import org.junit.Test;

public class StateTest {

    private static final Random RAND = new Random();

    private static final String
            PAN1 = "pubAgentName1",
            PAN2 = "pubAgentName2",
            SAI1 = "subAgentId1",
            SAI2 = "subAgentId2";

    @Test
    public void testEquality1() throws Exception {
        State state1 = new State(PAN1, SAI1, abs(RAND.nextLong()), 10, 100,  -1, false);
        State state2 = new State(PAN1, SAI1, abs(RAND.nextLong()), 10, 100,  -1, false);
        assertEquals(state1, state2);
    }

    @Test
    public void testEquality2() throws Exception {
        State state1 = new State(PAN1, SAI2, 0L, 0L, 0,  -1, false);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality3() throws Exception {
        State state1 = new State(PAN1, SAI2, 0L, 0L, 0,  -1, false);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality4() throws Exception {
        State state1 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        State state2 = new State(PAN2, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality5() throws Exception {
        State state1 = new State(PAN1, SAI1, 0L, 100L, 0,  -1, false);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality6() throws Exception {
        State state1 = new State(PAN1, SAI1, 0L, 0L, 100,  -1, false);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality7() throws Exception {
        State state1 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, true);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, false);
        assertNotEquals(state1, state2);
    }

    @Test
    public void testEquality8() throws Exception {
        State state1 = new State(PAN1, SAI1, 0L, 0L, 0,  -1, true);
        State state2 = new State(PAN1, SAI1, 0L, 0L, 0,  5, true);
        assertNotEquals(state1, state2);
    }

}