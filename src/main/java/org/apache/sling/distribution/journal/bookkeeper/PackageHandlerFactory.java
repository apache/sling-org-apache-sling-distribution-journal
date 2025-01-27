/*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * __________________
 *
 *  Copyright 2025 Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
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
