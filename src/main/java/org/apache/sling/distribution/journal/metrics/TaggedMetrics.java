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
package org.apache.sling.distribution.journal.metrics;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaggedMetrics {
    private static Logger log = LoggerFactory.getLogger(TaggedMetrics.class);
    
    private TaggedMetrics() {
    }

    public static String getMetricName(String metricName, List<Tag> tags) {
        String metric = tags.stream()
                .map(tag -> ";" + tag.getKey() + "=" + tag.getValue())
                .collect(Collectors.joining("", metricName, ""));
        log.debug("metric={}", metric);
        return metric;
    }
    
    public static String getMetricName(String metricName, Tag tag) {
        return getMetricName(metricName, Collections.singletonList(tag));
    }
}
