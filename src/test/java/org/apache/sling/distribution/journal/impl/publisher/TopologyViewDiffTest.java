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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.junit.Assert;
import org.junit.Test;

public class TopologyViewDiffTest {

    @Test
    public void testGetProcessedOffsetsNoChange() throws Exception {
        Assert.assertTrue(new TopologyViewDiff(buildView(buildState(-1)), buildView(buildState(-1)))
                .getProcessedOffsets().isEmpty());
        Assert.assertTrue(new TopologyViewDiff(buildView(buildState(0)), buildView(buildState(0)))
                .getProcessedOffsets().isEmpty());
        Assert.assertTrue(new TopologyViewDiff(buildView(buildState(100)), buildView(buildState(100)))
                .getProcessedOffsets().isEmpty());
    }

    @Test
    public void testGetProcessedOffsetsOneOffsetChange1() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(-1)), buildView(buildState(0)));
        Assert.assertEquals(0L, getMinProcessedOffset(diff));
        Assert.assertEquals(0L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testGetProcessedOffsetsOneOffsetChange2() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(0)), buildView(buildState(1)));
        Assert.assertEquals(1L, getMinProcessedOffset(diff));
        Assert.assertEquals(1L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testGetProcessedOffsetsOneOffsetChange3() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(100)), buildView(buildState(101)));
        Assert.assertEquals(101L, getMinProcessedOffset(diff));
        Assert.assertEquals(101L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testGetProcessedOffsetsRangeOffsetChange1() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(-1)), buildView(buildState(10)));
        Assert.assertEquals(0L, getMinProcessedOffset(diff));
        Assert.assertEquals(10L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testGetProcessedOffsetsRangeOffsetChange2() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(0)), buildView(buildState(10)));
        Assert.assertEquals(1L, getMinProcessedOffset(diff));
        Assert.assertEquals(10L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testGetProcessedOffsetsRangeOffsetChange3() throws Exception {
        TopologyViewDiff diff = new TopologyViewDiff(buildView(buildState(10)), buildView(buildState(100)));
        Assert.assertEquals(11L, getMinProcessedOffset(diff));
        Assert.assertEquals(100L, getMaxProcessedOffset(diff));
    }

    @Test
    public void testSubscribedAgentsChanged() {
        TopologyView view1 = buildView(
                new State("pub1", "sub1", 0, 10, 0, -1, false),
                new State("pub1", "sub2", 0, 5, 0, -1, false),
                new State("pub2", "sub1", 0, 100, 0, -1, false)
        );

        TopologyView view2 = buildView(
                new State("pub1", "sub1", 0, 10, 0, -1, false),
                // sub2 no longer in the view
                new State("pub2", "sub1", 0, 100, 0, -1, false)
        );

        TopologyViewDiff viewDiff1 = new TopologyViewDiff(view1, view1);
        assertFalse(viewDiff1.subscribedAgentsChanged());

        TopologyViewDiff viewDiff2 = new TopologyViewDiff(view1, view2);
        assertTrue(viewDiff2.subscribedAgentsChanged());
    }

    @Test
    public void testSubscribedAgentsChangedEmptyViews() {
        TopologyViewDiff viewDiff = new TopologyViewDiff(buildView(), buildView());
        assertFalse(viewDiff.subscribedAgentsChanged());
    }

    @Test
    public void testGetProcessedOffsetsBatch() throws Exception {
        TopologyView view1 = buildView(
                new State("pub1", "sub1", 0, 10, 0, -1, false),
                new State("pub1", "sub2", 0, 5, 0, -1, false),
                new State("pub2", "sub1", 0, 100, 0, -1, false),
                new State("pub2", "sub3", 0, 200, 0, -1, false),
                new State("pub3", "sub2", 0, 300, 0, -1, false),
                new State("pub3", "sub4", 0, 250, 0, -1, false)
        );

        TopologyView view2 = buildView(
                // sub1 and sub2 not in the view anymore
                // sub3 and sub4 subscribed to pub4
                new State("pub2", "sub3", 0, 1200, 0, -1, false),
                new State("pub3", "sub4", 0, 1250, 0, -1, false),
                new State("pub4", "sub3", 0, 1500, 0, -1, false),
                new State("pub4", "sub4", 0, 1550, 0, -1, false)
        );

        TopologyViewDiff viewDiff = new TopologyViewDiff(view1, view2);
        Map<String, Supplier<LongStream>> processedOffset = viewDiff.getProcessedOffsets();

        // only pub2 and pub3 are present in both views
        assertEquals(2, processedOffset.size());
        assertTrue(processedOffset.containsKey("pub2"));
        assertTrue(processedOffset.containsKey("pub3"));

        // check the offset stream fot pub2
        assertEquals(101, viewDiff.getProcessedOffsets().get("pub2").get().min().getAsLong());
        assertEquals(1200, viewDiff.getProcessedOffsets().get("pub2").get().max().getAsLong());

        // check the offset stream fot pub3
        assertEquals(251, viewDiff.getProcessedOffsets().get("pub3").get().min().getAsLong());
        assertEquals(1250, viewDiff.getProcessedOffsets().get("pub3").get().max().getAsLong());
    }

    @Test
    public void testGetProcessedOffsetsWrongOrder() throws Exception {
        TopologyView view1 = buildView(
                new State("pub1", "sub1", 0, 10, 0, -1, false)
        );

        TopologyView view2 = buildView(
                new State("pub1", "sub1", 0, 5, 0, -1, false)
        );

        TopologyViewDiff viewDiff = new TopologyViewDiff(view1, view2);
        Map<String, Supplier<LongStream>> processedOffset = viewDiff.getProcessedOffsets();

        assertEquals(0, processedOffset.size());
    }

    @Test
    public void testGetProcessedOffsetsEmptyViews() throws Exception {
        TopologyViewDiff viewDiff = new TopologyViewDiff(buildView(), buildView());
        Map<String, Supplier<LongStream>> processedOffset = viewDiff.getProcessedOffsets();
        assertEquals(0, processedOffset.size());
    }

    private TopologyView buildView(State ... state) {
        return new TopologyView(new HashSet<>(asList(state)));
    }

    private State buildState(long offset) {
        return new State("pub1", "sub1", System.currentTimeMillis(), offset, 0, -1, false);
    }

    private long getMinProcessedOffset(TopologyViewDiff diff) {
        return diff.getProcessedOffsets().get("pub1").get().min().orElseThrow(IllegalArgumentException::new);
    }

    private long getMaxProcessedOffset(TopologyViewDiff diff) {
        return diff.getProcessedOffsets().get("pub1").get().max().orElseThrow(IllegalArgumentException::new);
    }
}