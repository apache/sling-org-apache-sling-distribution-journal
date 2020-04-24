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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;

@Immutable
@ParametersAreNonnullByDefault
public class TopologyViewDiff {

    private final Map<String, Long> oldOffsets;

    private final Map<String, Long> newOffsets;

    private final boolean subAgentChanged;


    public TopologyViewDiff(TopologyView oldView, TopologyView newView) {
        oldOffsets = unmodifiableMap(oldView
                .getMinOffsetByPubAgentName());
        newOffsets = unmodifiableMap(newView
                .getMinOffsetByPubAgentName());
        subAgentChanged = ! Objects.equals(oldView.getSubscribedAgentIds(),
                newView.getSubscribedAgentIds());
    }

    /**
     * Return the map of the processed offsets, from the old and new view, per publisher agent.
     *
     * @return a map (pubAgentName x processedOffsets)
     */
    public Map<String, Supplier<LongStream>> getProcessedOffsets() {
        return retainedPubAgentNames().stream()
                .filter(this::newOffsetIsHigher)
                .collect(Collectors.toMap(identity(), this::offsetRange));
    }

    /**
     * @return {@code true} if the set of subscribed agent changed between the old and new view ;
     *         {@code false} otherwise.
     */
    public boolean subscribedAgentsChanged() {
        return subAgentChanged;
    }

    private boolean newOffsetIsHigher(String pubAgentName) {
        return newOffsets.get(pubAgentName) > oldOffsets.get(pubAgentName);
    }

    private Supplier<LongStream> offsetRange(String pubAgentName) {
        return () -> LongStream.rangeClosed(oldOffsets.get(pubAgentName) + 1, newOffsets.get(pubAgentName));
    }

    /**
     * @return the set of publisher agent names contained in both the old and new views
     */
    private Set<String> retainedPubAgentNames() {
        return retained(oldOffsets.keySet(), newOffsets.keySet());
    }

    private <T> Set<T> retained(Set<T> oldSet, Set<T> newSet) {
        Set<T> retained = new HashSet<>(newSet);
        retained.retainAll(oldSet);
        return retained;
    }
}
