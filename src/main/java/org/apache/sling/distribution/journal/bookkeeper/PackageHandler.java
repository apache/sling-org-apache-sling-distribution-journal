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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

class PackageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PackageHandler.class);
    
    private final DistributionPackageBuilder packageBuilder;
    
    private final ContentPackageExtractor extractor;

    private final BinaryStore binaryStore;

    public PackageHandler(DistributionPackageBuilder packageBuilder, ContentPackageExtractor extractor,
        BinaryStore binaryStore) {
        this.packageBuilder = packageBuilder;
        this.extractor = extractor;
        this.binaryStore = binaryStore;
    }

    public void apply(ResourceResolver resolver, PackageMessage pkgMsg)
            throws DistributionException, PersistenceException {
        PackageMessage.ReqType type = pkgMsg.getReqType();
        switch (type) {
            case ADD:
                installAddPackage(resolver, pkgMsg);
                break;
            case DELETE:
                installDeletePackage(resolver, pkgMsg);
                break;
            case TEST:
                break;
            default: throw new UnsupportedOperationException(format("Unable to process messages with type: %s", type));
        }
    }

    private void installAddPackage(ResourceResolver resolver, PackageMessage pkgMsg)
            throws DistributionException {
        LOG.debug("Importing paths {}",pkgMsg.getPaths());
        InputStream pkgStream = null;
        try {
            pkgStream = stream(resolver, pkgMsg, binaryStore);
            packageBuilder.installPackage(resolver, pkgStream);
            extractor.handle(resolver, pkgMsg.getPaths());
        } finally {
            IOUtils.closeQuietly(pkgStream);
        }

    }
    
    @Nonnull
    public static InputStream stream(ResourceResolver resolver, PackageMessage pkgMsg, BinaryStore binaryStore) throws DistributionException {
        if (pkgMsg.getPkgBinary() != null) {
            return new ByteArrayInputStream(pkgMsg.getPkgBinary());
        } else {
            String pkgBinRef = pkgMsg.getPkgBinaryRef();
            try {
                return binaryStore.get(pkgBinRef);
            } catch (IOException io) {
                throw new DistributionException(io.getMessage(), io);
            }
        }
    }

    private void installDeletePackage(ResourceResolver resolver, PackageMessage pkgMsg)
            throws PersistenceException {
        LOG.info("Deleting paths {}",pkgMsg.getPaths());
        for (String path : pkgMsg.getPaths()) {
            Resource resource = resolver.getResource(path);
            if (resource != null) {
                resolver.delete(resource);
            }
        }
    }
    
}
