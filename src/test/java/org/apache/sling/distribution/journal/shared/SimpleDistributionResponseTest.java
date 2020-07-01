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
package org.apache.sling.distribution.journal.shared;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.sling.distribution.DistributionRequestState;
import org.junit.Test;

public class SimpleDistributionResponseTest {


    @Test
    public void testGetStateAndMessage() throws Exception {
        String msg = "some message";
        DistributionRequestState state = DistributionRequestState.ACCEPTED;
        SimpleDistributionResponse response = new SimpleDistributionResponse(state, msg);
        assertEquals(msg, response.getMessage());
        assertEquals(state, response.getState());
    }
    
    @Test
    public void testStates() {
        assertTrue(isSuccessFul(DistributionRequestState.DISTRIBUTED));
        assertTrue(isSuccessFul(DistributionRequestState.ACCEPTED));
        assertFalse(isSuccessFul(DistributionRequestState.DROPPED));
        assertFalse(isSuccessFul(DistributionRequestState.NOT_EXECUTED));

    }

    private boolean isSuccessFul(DistributionRequestState state) {
        return new SimpleDistributionResponse(state, "").isSuccessful();
    }
}