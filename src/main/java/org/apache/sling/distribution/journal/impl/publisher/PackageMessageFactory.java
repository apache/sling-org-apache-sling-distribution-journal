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
package org.apache.sling.distribution.journal.impl.publisher;

import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.PackageMessageBuilder;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = PackageMessageFactory.class)
@ParametersAreNonnullByDefault
public class PackageMessageFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PackageMessageFactory.class);

    private static final long MAX_INLINE_PKG_BINARY_SIZE = 800L * 1024;

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private PackageRepo packageRepo;

    private String pubSlingId;

    public PackageMessageFactory() {}

    public PackageMessageFactory(
            String pubSlingId
            ) {
        this.pubSlingId = pubSlingId;
    }

    @Activate
    public void activate() {
        pubSlingId = slingSettings.getSlingId();
        LOG.info("Started package message factory for pubSlingId {}", pubSlingId);
    }

    @Deactivate
    public void deactivate() {
        LOG.info("Stopped package message factory for pubSlingId {}", pubSlingId);
    }

    public PackageMessage create(
            DistributionPackageBuilder packageBuilder,
            ResourceResolver resourceResolver,
            String pubAgentName,
            DistributionRequest request)
                    throws DistributionException {
        switch (request.getRequestType()) {
            case ADD: return createAdd(packageBuilder, resourceResolver, pubAgentName, request);
            case DELETE: return createDelete(packageBuilder, resourceResolver, request, pubAgentName);
            case TEST: return createTest(packageBuilder, resourceResolver, pubAgentName);
            default: throw new IllegalArgumentException(String.format("Unsupported request type %s", request.getRequestType()));
        }
    }

    @Nonnull
    private PackageMessage createAdd(DistributionPackageBuilder packageBuilder, ResourceResolver resourceResolver, String pubAgentName, DistributionRequest request) throws DistributionException {
        final DistributionPackage disPkg = requireNonNull(packageBuilder.createPackage(resourceResolver, request));
        final byte[] pkgBinary = pkgBinary(disPkg);
        long pkgLength = pkgBinary.length;
        final DistributionPackageInfo pkgInfo = disPkg.getInfo();
        final List<String> paths = Arrays.asList(pkgInfo.getPaths());
        final List<String> deepPaths = Arrays.asList(pkgInfo.get(PROPERTY_REQUEST_DEEP_PATHS, String[].class));
        final String pkgId = disPkg.getId();
        PackageMessageBuilder pkgBuilder = PackageMessage.builder()
                .pubSlingId(pubSlingId)
                .pkgId(pkgId)
                .pubAgentName(pubAgentName)
                .paths(paths)
                .reqType(ReqType.ADD)
                .deepPaths(deepPaths)
                .pkgLength(pkgLength)
                .userId(resourceResolver.getUserID())
                .pkgType(packageBuilder.getType());
        if (pkgLength > MAX_INLINE_PKG_BINARY_SIZE) {

            /*
             * Rather than pro-actively (and somewhat arbitrarily)
             * decide to avoid sending a package inline based on
             * its size, we could simply try to send packages of
             * any size and only avoiding to inline as a fallback.
             * However, this approach requires the messaging
             * implementation to offer a mean to distinguish
             * size issues when sending messages, which is not
             * always the case.
             */

            LOG.info("Package {} too large ({}B) to be sent inline", disPkg.getId(), pkgLength);
            String pkgBinRef = packageRepo.store(resourceResolver, disPkg);
            pkgBuilder.pkgBinaryRef(pkgBinRef);
        } else {
            pkgBuilder.pkgBinary(pkgBinary);
        }
        PackageMessage pipePackage = pkgBuilder.build();
        disPkg.delete();
        return pipePackage;
    }
    
    @Nonnull
    private PackageMessage createDelete(DistributionPackageBuilder packageBuilder, ResourceResolver resourceResolver, DistributionRequest request, String pubAgentName) {
        String pkgId = UUID.randomUUID().toString();
        return PackageMessage.builder()
                .pubSlingId(pubSlingId)
                .pkgId(pkgId)
                .pubAgentName(pubAgentName)
                .paths(Arrays.asList(request.getPaths()))
                .reqType(ReqType.DELETE)
                .pkgType(packageBuilder.getType())
                .userId(resourceResolver.getUserID())
                .build();
    }

    @Nonnull
    public PackageMessage createTest(DistributionPackageBuilder packageBuilder, ResourceResolver resourceResolver, String pubAgentName) {
        String pkgId = UUID.randomUUID().toString();
        return PackageMessage.builder()
                .pubSlingId(pubSlingId)
                .pubAgentName(pubAgentName)
                .pkgId(pkgId)
                .reqType(ReqType.TEST)
                .pkgType(packageBuilder.getType())
                .userId(resourceResolver.getUserID())
                .build();
    }

    @Nonnull
    private byte[] pkgBinary(DistributionPackage pkg) {
        try {
            return IOUtils.toByteArray(pkg.createInputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
