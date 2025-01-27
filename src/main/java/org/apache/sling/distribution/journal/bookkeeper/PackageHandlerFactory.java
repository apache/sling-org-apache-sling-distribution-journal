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
package org.apache.sling.distribution.journal.bookkeeper;

import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = PackageHandlerFactory.class)
public class PackageHandlerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PackageHandlerFactory.class);

    @Reference
    private BundleContext bundleContext;

    @Reference(name="default")
    PackageHandler defaultPackageHandler;

    public PackageHandler create(String name) {
        if (StringUtils.isEmpty(name)) {
            return defaultPackageHandler;
        }
        Collection<ServiceReference<PackageHandler>> serviceReferences;
        try {
            serviceReferences = bundleContext.getServiceReferences(PackageHandler.class, null);
            for (ServiceReference<PackageHandler> serviceReference : serviceReferences) {
                PackageHandler packageHandler = bundleContext.getService(serviceReference);
                if (packageHandler != null && serviceReference.getProperty("name").equals(name)) {
                    return packageHandler;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while creating package handler", e);
        }
        return defaultPackageHandler;
    }
}
