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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService.GaugeService;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        immediate = true,
        service = EventHandler.class,
        property = EventConstants.EVENT_TOPIC + "=" + ExceptionEventSender.ERROR_TOPIC
)
@Designate(ocd = JournalAvailableChecker.JournalCheckerConfiguration.class)
public class JournalAvailableChecker implements EventHandler {

    public static final long INITIAL_RETRY_DELAY = 60000; // 1 minute

    public static final long MAX_RETRY_DELAY = 1800000; // 30 minutes

    private static final Logger LOG = LoggerFactory.getLogger(JournalAvailableChecker.class);
    
    private ExponentialBackOff backoffRetry;
    
    @Reference
    Topics topics;
    
    @Reference
    MessagingProvider provider;
    
    @Reference
    DistributionMetricsService metrics;
    
    JournalAvailableServiceMarker marker;

    private GaugeService<Boolean> gauge;

    @Activate
    public void activate(JournalCheckerConfiguration config, BundleContext context) {
        requireNonNull(provider);
        requireNonNull(topics);
        this.backoffRetry = new ExponentialBackOff(config.initialRetryDelay(), config.maxRetryDelay(), true, this::run);
        this.marker = new JournalAvailableServiceMarker(context);
        this.gauge = metrics.createGauge(DistributionMetricsService.BASE_COMPONENT + ".journal_available", "", this::isAvailable);

        Arrays.asList(config.trackedErrCodes()).stream().spliterator()
            .forEachRemaining(code -> metrics.getJournalErrorCodeCount(code));

        this.marker.register();
        LOG.info("Started Journal availability checker service with initialRetryDelay {}, maxRetryDelay {}. Journal is initially assumed available.", config.initialRetryDelay(), config.maxRetryDelay());
    }

    @Deactivate
    public void deactivate() {
        gauge.close();
        this.marker.unRegister();
        IOUtils.closeQuietly(this.backoffRetry);
        LOG.info("Stopped Journal availability checker service");
    }

    private void doChecks() {
        provider.assertTopic(topics.getPackageTopic());
        provider.assertTopic(topics.getDiscoveryTopic());
        provider.assertTopic(topics.getStatusTopic());
        provider.assertTopic(topics.getCommandTopic());
    }

    private void available() {
        LOG.info("Journal is available");
        this.marker.register();
    }

    private void stillUnAvailable(Exception e) {
        String msg = "Journal is still unavailable: " + e.getMessage();
        LOG.warn(msg, e);
        this.marker.unRegister();
    }
    
    public boolean isAvailable() {
        return this.marker.isRegistered();
    }

    public void run() {
        try {
            LOG.debug("Journal checker is running");
            doChecks();
            available();
        } catch (Exception e) {
            stillUnAvailable(e);
            throw e;
        }
    }

    @Override
    public synchronized void handleEvent(Event event) {
        String type = (String) event.getProperty(ExceptionEventSender.KEY_TYPE);
        String msg = (String) event.getProperty(ExceptionEventSender.KEY_MESSAGE);
        if (this.marker.isRegistered()) {
            LOG.warn("Received exception event {}: {}. Journal is considered unavailable.", type, msg);
            this.marker.unRegister();
            this.backoffRetry.startChecks();
        } else {
            LOG.info("Received exception event {}: {}. Journal still unavailable.", type, msg);
        }
        String errCode = (String) event.getProperty(ExceptionEventSender.KEY_ERROR_CODE);
        if ((errCode != null) && !errCode.isEmpty()) {
            metrics.getJournalErrorCodeCount(errCode).increment();
        }
    }

    @ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Journal Checker")
    public @interface JournalCheckerConfiguration {

        @AttributeDefinition(name = "Initial retry delay",
                description = "The initial retry delay in milliseconds.")
        long initialRetryDelay() default INITIAL_RETRY_DELAY;

        @AttributeDefinition(name = "Max retry delay",
                description = "The max retry delay in milliseconds.")
        long maxRetryDelay() default MAX_RETRY_DELAY;

        @AttributeDefinition(name ="Tracked response codes",
            description = "Response error codes tracked in metrics.")
        String[] trackedErrCodes() default {"400", "401", "404", "405", "413", "500", "503", "505"};
    }
}
