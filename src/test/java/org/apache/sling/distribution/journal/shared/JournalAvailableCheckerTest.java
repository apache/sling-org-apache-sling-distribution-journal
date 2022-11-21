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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.commons.metrics.Counter;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.shared.JournalAvailableChecker.JournalCheckerConfiguration;
import org.apache.sling.distribution.journal.shared.Topics.TopicsConfiguration;
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

    @Before
    public void before() throws Exception {
        MessagingException me = new MessagingException("expected", new RuntimeException("expected nested exception"));
        doThrow(me)
            .when(provider).assertTopic(INVALID_TOPIC);
        when(context.registerService(Mockito.eq(JournalAvailable.class), Mockito.any(JournalAvailable.class), Mockito.any()))
                .thenReturn(sreg);
        Map<String, String> config = new HashMap<>();
        config.put("initialRetryDelay", "500");
        config.put("maxRetryDelay", "300000");
        checker.activate(configuration(config, JournalCheckerConfiguration.class), context);
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
        activateChecksOnEvent(new MessagingException("Expected", "400"), 1);
    }

    @Test
    public void testActivateChecksOnEventNoErrCode() throws InterruptedException {
        activateChecksOnEvent(new MessagingException("Expected"), 0);
    }

    @Test
    public void testActivateChecksOnEventOtherException() throws InterruptedException {
        activateChecksOnEvent(new IOException("Expected"), 0);
    }

    @Test
    public void testActivateChecksOnEventWithoutMessage() throws InterruptedException {
        activateChecksOnEvent(new IOException(), 0);
    }

    public void activateChecksOnEvent(Exception e, int expectedCounter) throws InterruptedException {
        Counter counter = new TestCounter();
        when(metrics.getJournalErrorCodeCount("400")).thenReturn(counter);

        await("At the start checks are triggers and should set the state available")
            .until(checker::isAvailable);

        makeCheckFail();
        Event event = createErrorEvent(e);
        checker.handleEvent(event);
        assertEquals(expectedCounter, counter.getCount());
        await().until(() -> !checker.isAvailable());
        Thread.sleep(1000); // Make sure we get at least one failed doCheck
        makeCheckSucceed();
        await().until(checker::isAvailable);
    }

    private void makeCheckSucceed() {
        topics.activate(configuration(emptyMap(), TopicsConfiguration.class));
    }

    private void makeCheckFail() {
        topics.activate(configuration(singletonMap("packageTopic", INVALID_TOPIC), TopicsConfiguration.class));
    }

    private <T> T configuration(Map<String,String> props, Class<T> clazz) {
        return standardConverter()
                .convert(props)
                .to(clazz);
    }

    private static Event createErrorEvent(Exception e) {
        Map<String, String> props = new HashMap<>();
        props.put(ExceptionEventSender.KEY_TYPE, e.getClass().getName());
        props.put(ExceptionEventSender.KEY_MESSAGE, e.getMessage());
        if (e instanceof MessagingException) {
            props.put(ExceptionEventSender.KEY_ERROR_CODE, ((MessagingException) e).getResponseCode());
        }
        return new Event(ExceptionEventSender.ERROR_TOPIC, props);
    }

    class TestCounter implements Counter {
        AtomicLong l = new AtomicLong();
        @Override public void increment() {
            l.getAndIncrement();
        }

        @Override public void decrement() {
            l.decrementAndGet();
        }

        @Override public void increment(long n) {
            l.addAndGet(n);
        }

        @Override public void decrement(long n) {
            l.addAndGet(-n);
        }

        @Override public long getCount() {
            return l.get();
        }

        @Override public <A> A adaptTo(Class<A> type) {
            return null;
        }
    }
}
