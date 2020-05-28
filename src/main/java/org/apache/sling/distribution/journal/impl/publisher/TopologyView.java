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

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.ToStringBuilder;

@Immutable
@ParametersAreNonnullByDefault
public class TopologyView {

    private final Set<State> states;

    public TopologyView() {
        this(Collections.emptySet());
    }

    public TopologyView(Set<State> newStates) {
        this.states = unmodifiableSet(newStates);
    }

    /**
     * Return the identifiers of subscriber agents
     *
     * @return a set of subscriber agent identifiers (subAgentId)
     */
    public Set<String> getSubscribedAgentIds() {
        return states.stream().map(State::getSubAgentId).collect(toSet());
    }

    /**
     * Return the identifiers of subscriber agents subscribed to the given publisher agent.
     *
     * @param pubAgentName the name of the publisher agent
     * @return a set of subscriber agent identifiers (subAgentId)
     */
    public Set<String> getSubscribedAgentIds(String pubAgentName) {
        return states.stream()
                .filter(state -> state.getPubAgentName().equals(pubAgentName))
                .map(State::getSubAgentId)
                .collect(toSet());
    }
    
    /**
     * Return the identifiers of subscriber agents subscribed to the given publisher agent.
     *
     * @param pubAgentName the name of the publisher agent
     * @return a set of subscriber agent identifiers (subAgentId)
     */
    public Set<State> getSubscribedAgents(String pubAgentName) {
        return states.stream()
                .filter(state -> state.getPubAgentName().equals(pubAgentName))
                .collect(toSet());
    }

    /**
     * Return the states for the given subscriber agent.
     *
     * @param subAgentId the name of the subscriber agent to get the states for
     * @return states of that subscriber agent
     */
    public Set<State> getSubscriberAgentStates(String subAgentId) {
        return states.stream().filter(state -> state.getSubAgentId().equals(subAgentId)).collect(toSet());
    }

    @CheckForNull
    public State getState(String subAgentId, String pubAgentName) {
        return states.stream()
                .filter(state -> state.getSubAgentId().equals(subAgentId))
                .filter(state -> state.getPubAgentName().equals(pubAgentName))
                .findFirst().orElse(null);
    }

    /**
     * Return the map of min offset per publisher agent
     *
     * @return a map (pubAgentName x offset)
     */
    public Map<String, Long> getMinOffsetByPubAgentName() {
        return states.stream()
                .collect(groupingBy(State::getPubAgentName, reducing(Long.MAX_VALUE, State::getOffset, Long::min)));

    }

    /**
     * Return a stream of the offsets from all states in the view.
     *
     * @return a stream of offsets
     */
    public Stream<Long> offsets() {
        return states.stream().map(State::getOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopologyView that = (TopologyView) o;
        return Objects.equals(states, that.states);
    }

    @Override
    public int hashCode() {
        return Objects.hash(states);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, JSON_STYLE)
                .append("states", states)
                .toString();
    }
}
