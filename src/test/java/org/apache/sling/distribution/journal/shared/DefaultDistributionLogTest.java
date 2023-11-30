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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.sling.distribution.journal.impl.publisher.DistributionPublisher;
import org.apache.sling.distribution.journal.shared.DefaultDistributionLog.LogLevel;
import org.junit.Test;

public class DefaultDistributionLogTest {

    @Test
    public void testTime() throws InterruptedException {
        DefaultDistributionLog log = new DefaultDistributionLog("name", DistributionPublisher.class, LogLevel.INFO);
        log.info("Test1");
        Thread.sleep(2);
        log.info("Test2");
        List<String> lines = log.getLines().stream().map(this::getTime).collect(Collectors.toList());
        String time1 = lines.get(0);
        String time2 = lines.get(1);
        System.out.println(time1);
        assertThat("Times of log entries must be different " + time1 + " " + time2, time1, not(equalTo(time2)));
    }
    
    String getTime(String logLine) {
        return logLine.split(" ")[1];
    }
}
