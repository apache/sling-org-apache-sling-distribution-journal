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

import java.util.Map;

import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.osgi.util.converter.Converters.standardConverter;

public class JournalAvailableCheckerTest {

    private static final String INVALID_TOPIC = "invalid_topic_name";

    private JournalAvailableChecker checker;

    @Spy
    private Topics topics = new Topics();

    @Mock
    private MessagingProvider provider;

    @Mock
    private BundleContext context;

    @Mock
    private ServiceRegistration<JournalAvailable> reg;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);
        doThrow(new MessagingException("topic is invalid"))
                .when(provider).assertTopic(INVALID_TOPIC);
        when(context.registerService(Mockito.eq(JournalAvailable.class), Mockito.any(JournalAvailable.class), Mockito.any()))
                .thenReturn(reg);
        checker = new JournalAvailableChecker(provider, topics);
        checker.activate(context);
    }

    @After
    public void after() throws Exception {
        checker.deactivate();
    }

    @Test
    public void testIsAvailable() throws Exception {
        topics.activate(topicsConfiguration(singletonMap("packageTopic", INVALID_TOPIC)));
        checker.run();
        assertFalse(checker.isAvailable());
        topics.activate(topicsConfiguration(emptyMap()));
        checker.run();
        assertTrue(checker.isAvailable());
    }

    private Topics.TopicsConfiguration topicsConfiguration(Map<String,String> props) {
        return standardConverter()
                .convert(props)
                .to(Topics.TopicsConfiguration.class);
    }

}