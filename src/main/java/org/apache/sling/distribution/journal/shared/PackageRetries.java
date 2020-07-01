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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Holds package retries by agent name
 */
@ParametersAreNonnullByDefault
public class PackageRetries {

    // (pubAgentName x retries)
    private final Map<String, Integer> pubAgentNameToRetries = new ConcurrentHashMap<>();

    public void increase(String pubAgentName) {
        pubAgentNameToRetries.merge(pubAgentName, 1, Integer::sum);
    }

    public void clear(String pubAgentName) {
        pubAgentNameToRetries.remove(pubAgentName);
    }

    public int get(String pubAgentName) {
        return pubAgentNameToRetries.getOrDefault(pubAgentName, 0);
    }

    public int getSum() {
        return pubAgentNameToRetries.values().stream().mapToInt(Integer::intValue).sum();
    }
}
