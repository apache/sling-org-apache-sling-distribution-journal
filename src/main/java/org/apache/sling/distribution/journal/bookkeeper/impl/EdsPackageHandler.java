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
package org.apache.sling.distribution.journal.bookkeeper.impl;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.bookkeeper.ContentPackageExtractor;
import org.apache.sling.distribution.journal.bookkeeper.PackageHandler;
import org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriber;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

@Component(service = PackageHandler.class, name = "edge-delivery")
public class EdsPackageHandler implements PackageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EdsPackageHandler.class);

    @Override
    public void apply(ResourceResolver resolver, PackageMessage pkgMsg, ContentPackageExtractor extractor)
            throws DistributionException, PersistenceException {
        PackageMessage.ReqType type = pkgMsg.getReqType();
        switch (type) {
            case ADD:
                // update preview and publish in EDS
                LOG.info("TODO: update preview and publish in EDS");
                break;
            case DELETE:
                // delete preview and publish in EDS
                LOG.info("TODO: delete preview and publish in EDS");
                break;
            case TEST:
                break;
            default: throw new UnsupportedOperationException(format("Unable to process messages with type: %s", type));
        }
    }
}
