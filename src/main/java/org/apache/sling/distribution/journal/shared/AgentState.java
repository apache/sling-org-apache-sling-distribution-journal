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

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.agent.DistributionAgentState;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.queue.DistributionQueueState;
import org.apache.sling.distribution.queue.DistributionQueueStatus;
import org.apache.sling.distribution.queue.spi.DistributionQueue;

import static org.apache.sling.distribution.agent.DistributionAgentState.BLOCKED;
import static org.apache.sling.distribution.agent.DistributionAgentState.IDLE;
import static org.apache.sling.distribution.agent.DistributionAgentState.RUNNING;

@ParametersAreNonnullByDefault
public class AgentState {
    
    private AgentState() {
    }

    public static DistributionAgentState getState(DistributionAgent agent) {
        boolean empty = queueStatuses(agent).noneMatch(AgentState::queueNotEmpty);
        if (empty) {
            return IDLE;
        }
        boolean blocked = queueStatuses(agent).anyMatch(AgentState::isBlocked);
        if (blocked) {
            return BLOCKED;
        }
        return RUNNING;
    }

    private static Stream<DistributionQueueStatus> queueStatuses(DistributionAgent agent) {
        return StreamSupport.stream(agent.getQueueNames().spliterator(), true)
                .map(agent::getQueue)
                .map(DistributionQueue::getStatus);
    }

    private static boolean queueNotEmpty(DistributionQueueStatus queueStatus) {
        return ! queueStatus.isEmpty();
    }

    private static boolean isBlocked(DistributionQueueStatus queueStatus) {
        return queueStatus.getState() == DistributionQueueState.BLOCKED;
    }
}
