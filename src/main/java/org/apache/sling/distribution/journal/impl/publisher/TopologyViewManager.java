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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class TopologyViewManager {

    /**
     * (stateId x State)
     */
    private final Map<String, State> states = new ConcurrentHashMap<>();

    private volatile TopologyView currentView = new TopologyView();

    private final long refreshTtl;

    /**
     * @param refreshTtl the time to live (in ms) for States without being refreshed.
     */
    public TopologyViewManager(long refreshTtl) {
        this.refreshTtl = refreshTtl;
    }

    public TopologyView getCurrentView() {
        return currentView;
    }

    public void refreshState(State state) {
        states.put(stateId(state), state);
    }

    /**
     * Update the Topology view
     *
     * @return the Topology view before updating.
     */
    public TopologyView updateView() {
        long now = System.currentTimeMillis();
        return updateView(now);
    }

    protected TopologyView updateView(long now) {
        TopologyView oldView = this.currentView;
        currentView = buildView(now);
        return oldView;
    }

    protected int size() {
        return states.size();
    }

    private TopologyView buildView(long now) {
        Set<State> newStates = states.values().stream()
                .filter(e -> isAlive(now, e))
                .collect(Collectors.toSet());
        return new TopologyView(newStates);
    }

    private boolean isAlive(long now, State state) {
        return now - state.getTimestamp() < refreshTtl;
    }

    private String stateId(State state) {
        return String.format("%s#%s", state.getSubAgentId(), state.getPubAgentName());
    }

}
