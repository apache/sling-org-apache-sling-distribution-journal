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

import javax.annotation.ParametersAreNonnullByDefault;

import static java.util.Objects.requireNonNull;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_IMMEDIATE;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.JournalAvailable;

@Component(
        service = {Runnable.class},
        immediate = true,
        property = {
                PROPERTY_SCHEDULER_IMMEDIATE + ":Boolean=true",
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 90  // 90 seconds
        }
)
@ParametersAreNonnullByDefault
public class JournalAvailableChecker implements JournalAvailable, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(JournalAvailableChecker.class);

    @Reference
    Topics topics;
    
    @Reference
    MessagingProvider provider;

    private BundleContext context;

    private volatile ServiceRegistration<JournalAvailable> reg;

    public JournalAvailableChecker(){

    }

    public JournalAvailableChecker(MessagingProvider provider, Topics topics) {
        this.provider = provider;
        this.topics = topics;
    }

    @Activate
    public void activate(BundleContext context) {
        requireNonNull(provider);
        requireNonNull(topics);
        this.context = context;
        LOG.info("Started Journal availability checker service");
    }

    @Deactivate
    public void deactivate() {
        unRegister();
        LOG.info("Stopped Journal availability checker service");
    }

    private void doChecks() {
        provider.assertTopic(topics.getPackageTopic());
        provider.assertTopic(topics.getDiscoveryTopic());
        provider.assertTopic(topics.getStatusTopic());
        provider.assertTopic(topics.getCommandTopic());
    }

    private void available() {
        if (this.reg == null) {
            LOG.info("Journal is available");
            this.reg = context.registerService(JournalAvailable.class, this, null);
        }
    }
    
    private void unAvailable(Exception e) {
        if (LOG.isDebugEnabled()) {
            LOG.warn("Journal is unavailable " + e.getMessage(), e);
        } else {
            LOG.warn("Journal is unavailable " + e.getMessage());
        }
        unRegister();
    }
    
    public boolean isAvailable() {
        return reg != null;
    }

    @Override
    public void run() {
        try {
            LOG.debug("Journal checker is running");
            doChecks();
            available();
        } catch (Exception e) {
            unAvailable(e);
        }
    }

    private void unRegister() {
        if (this.reg != null) {
            this.reg.unregister();
            this.reg = null;
        }
    }
}
