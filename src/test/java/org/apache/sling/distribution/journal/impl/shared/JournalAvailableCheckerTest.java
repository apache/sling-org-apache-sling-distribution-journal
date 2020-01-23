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
package org.apache.sling.distribution.journal.impl.shared;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService.GaugeService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.Event;

@RunWith(MockitoJUnitRunner.class)
public class JournalAvailableCheckerTest {

    private static final String INVALID_TOPIC = "invalid_topic_name";

    @InjectMocks
    private JournalAvailableChecker checker;
    
    @Spy
    private Topics topics = new Topics();

    @Mock
    private MessagingProvider provider;
    
    @Mock
    private DistributionMetricsService metrics;

    @Mock
    private BundleContext context;

    @Mock
    private ServiceRegistration<JournalAvailable> sreg;

    @SuppressWarnings("rawtypes")
    @Mock
    private GaugeService gauge;
    
    @SuppressWarnings("unchecked")
    @Before
    public void before() throws Exception {
        when(metrics.createGauge(Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(gauge);
        doThrow(new MessagingException("topic is invalid", new RuntimeException("Nested exception")))
                .when(provider).assertTopic(INVALID_TOPIC);
        when(context.registerService(Mockito.eq(JournalAvailable.class), Mockito.any(JournalAvailable.class), Mockito.any()))
                .thenReturn(sreg);
        checker.activate(context);
    }

    @After
    public void after() throws Exception {
        checker.deactivate();
    }

    @Test
    public void testIsAvailable() throws Exception {
        makeCheckFail();
        try {
            checker.run();
            Assert.fail("Should throw exception");
        } catch (Exception e) {
        }
        assertFalse(checker.isAvailable());
        makeCheckSucceed();
        checker.run();
        assertTrue(checker.isAvailable());
    }

    @Test
    public void testActivateChecksOnEvent() throws InterruptedException {
        await("At the start checks are triggers and should set the state available")
            .until(checker::isAvailable);
        
        makeCheckFail();
        Event event = createErrorEvent(new IOException("Expected"));
        checker.handleEvent(event);
        await().until(() -> !checker.isAvailable());
        Thread.sleep(1000); // Make sure we get at least one failed doCheck
        makeCheckSucceed();
        await().until(checker::isAvailable);
    }
    
    private void makeCheckSucceed() {
        topics.activate(topicsConfiguration(emptyMap()));
    }

    private void makeCheckFail() {
        topics.activate(topicsConfiguration(singletonMap("packageTopic", INVALID_TOPIC)));
    }

    private Topics.TopicsConfiguration topicsConfiguration(Map<String,String> props) {
        return standardConverter()
                .convert(props)
                .to(Topics.TopicsConfiguration.class);
    }

    private static Event createErrorEvent(Exception e) {
        Map<String, String> props = new HashMap<>();
        props.put(ExceptionEventSender.KEY_TYPE, e.getClass().getName());
        props.put(ExceptionEventSender.KEY_MESSAGE, e.getMessage());
        return new Event(ExceptionEventSender.ERROR_TOPIC, props);
    }
}