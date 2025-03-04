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

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.JournalDistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.commons.io.IOUtils.toByteArray;
import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.ADD;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_TYPE;

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
        DistributionPackage distributionPackage = toDistributionPackage(pkgMsg);
        packageBuilder.installPackage(resolver, distributionPackage);
        ReqType type = pkgMsg.getReqType();
        if (type == ADD) {
            extractor.handle(resolver, pkgMsg.getPaths());
        }
    }

    private DistributionPackage toDistributionPackage(PackageMessage pkgMsg)
            throws DistributionException {
        LOG.debug("Importing paths {}",pkgMsg.getPaths());
        final byte[] data;
        try (InputStream inputStream = stream(pkgMsg)) {
            data = toByteArray(inputStream);
        } catch (IOException e) {
            throw new DistributionException("Failed to download package from binary store", e);
        }
        DistributionPackageInfo distributionPackageInfo = new DistributionPackageInfo(pkgMsg.getPkgType());
        distributionPackageInfo.put(PROPERTY_REQUEST_PATHS, pkgMsg.getPaths().toArray());
        distributionPackageInfo.put(PROPERTY_REQUEST_DEEP_PATHS, pkgMsg.getDeepPaths().toArray());
        distributionPackageInfo.put(PROPERTY_REQUEST_TYPE, pkgMsg.getReqType());
        return new JournalDistributionPackage(pkgMsg.getPkgId(), pkgMsg.getPkgType(), data, distributionPackageInfo);
    }

    @Nonnull
    InputStream stream(PackageMessage pkgMsg) throws DistributionException {
        if (pkgMsg.getPkgBinary() != null) {
            return new ByteArrayInputStream(pkgMsg.getPkgBinary());
        }
        String pkgBinRef = pkgMsg.getPkgBinaryRef();
        if (pkgBinRef != null) {
            try {
                return binaryStore.get(pkgBinRef);
            } catch (IOException io) {
                throw new DistributionException(io.getMessage(), io);
            }
        }
        return new ByteArrayInputStream(new byte[0]);
    }
}
