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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

public class TaggedMetricsTest {
    @Test
    public void testNoTags() {
        String metricName = TaggedMetrics.getMetricName("metric", Collections.emptyList());
        assertThat(metricName, equalTo("metric"));
    }

    @Test
    public void testTags() {
        Tag tag1 = Tag.of("key1", "value1");
        Tag tag2 = Tag.of("key2", "value2");
        String metricName = TaggedMetrics.getMetricName("metric", Arrays.asList(tag1, tag2));
        assertThat(metricName, equalTo("metric;key1=value1;key2=value2"));
    }
}
