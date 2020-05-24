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

import java.io.IOException;

import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.cm.ConfigurationEvent;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_CONCURRENT;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_IMMEDIATE;
import static org.apache.sling.commons.scheduler.Scheduler.PROPERTY_SCHEDULER_PERIOD;

@Component(
        property = {
                PROPERTY_SCHEDULER_CONCURRENT + ":Boolean=false",
                PROPERTY_SCHEDULER_IMMEDIATE + ":Boolean=false",
                PROPERTY_SCHEDULER_PERIOD + ":Long=" + 60, // 1 minute
        }
)
public class DistributionPublisherConfigured implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionPublisherConfigured.class);

    private volatile BundleContext context;

    private ServiceRegistration<DistributionPublisherConfigured> reg;

    private final Object lock = new Object();

    @Reference
    private ConfigurationAdmin configAdmin;

    @Activate
    public void activate(BundleContext context) {
        this.context = context;
    }

    @Deactivate
    public void deactivate() {
        synchronized (lock) {
            if (reg != null) {
                reg.unregister();
                LOG.info("Unregistered marker service");
            }
        }
    }

    @Override
    public void configurationEvent(ConfigurationEvent event) {
        if ("org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory".equals(event.getFactoryPid())
                && event.getType() != ConfigurationEvent.CM_DELETED) {
            synchronized (lock) {
                if (reg == null) {
                    reg = context.registerService(DistributionPublisherConfigured.class, this, null);
                    LOG.info("Registered marker service");
                }
            }
        }
    }

    @Override
    public void run() {

        String filter = "(" + Constants.SERVICE_PID + "=" + "org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory" + ")";
        try {
            Configuration[] configs = configAdmin.listConfigurations(filter);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidSyntaxException e) {
            e.printStackTrace();
        }
        if () {

        }
    }

    private void publisherConfigurationsAvailable() {

    }
}
