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

import java.util.Dictionary;

import org.apache.sling.distribution.journal.impl.publisher.DistributionPublisher;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service checks for DistributionPublisher agent
 * configuration availability and registers the marker service
 * {@link PublisherConfigurationAvailable} when such configuration
 * could be found. To avoid costly reactivation cycles, the marker
 * service remains registered until this service is deactivated.
 * This service is meant to be executed on every instance, even in a cluster.
 */
@Component(
        immediate = true,
        property = { Constants.SERVICE_PID + "=" + DistributionPublisher.FACTORY_PID}
)
public class PublisherConfigurationAvailable implements ManagedServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PublisherConfigurationAvailable.class);

    private volatile ServiceRegistration<PublisherConfigurationAvailable> reg; //NOSONAR

    private volatile BundleContext context; //NOSONAR

    private final Object lock = new Object();

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

    protected boolean isAvailable() {
        return reg != null;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void updated(String pid, Dictionary<String, ?> properties) throws ConfigurationException {
        synchronized (lock) {
            if (reg == null) {
                reg = context.registerService(PublisherConfigurationAvailable.class, this, null);
                LOG.info("Registered marker service");
            }
        }
    }

    @Override
    public void deleted(String pid) {
        // We keep the registration up even if the config goes away
    }
}
