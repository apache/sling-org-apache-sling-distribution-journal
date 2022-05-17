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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
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
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        service = PackageMessageFactory.class,
        immediate = true,
        configurationPid = PackageMessageFactory.FACTORY_PID
)
@Designate(ocd = PackageFactoryConfiguration.class, factory = true)
@ParametersAreNonnullByDefault
public class PackageMessageFactory {

    public static final String FACTORY_PID = "org.apache.sling.distribution.journal.impl.publisher.PackageMessageFactory";

    private static final Logger LOG = LoggerFactory.getLogger(PackageMessageFactory.class);

    @Reference
    private SlingSettingsService slingSettings;

    @Reference
    private BinaryStore binaryStore;

    private String pubSlingId;

    private long maxPackageSize = -1;

    public PackageMessageFactory() {}

    @Activate
    public void activate(PackageFactoryConfiguration packageFactoryConfiguration) {
        maxPackageSize = packageFactoryConfiguration.maxPackageSize();
        pubSlingId = slingSettings.getSlingId();
        LOG.info("Started package message factory for pubSlingId={}, maxPackageSize={}", pubSlingId, maxPackageSize);
    }

    @Deactivate
    public void deactivate() {
        LOG.info("Stopped package message factory for pubSlingId={}", pubSlingId);
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
            case INVALIDATE: return createInvalidate(packageBuilder, resourceResolver, request, pubAgentName);
            case TEST: return createTest(packageBuilder, resourceResolver, pubAgentName);
            default: throw new IllegalArgumentException(format("Unsupported request with requestType=%s", request.getRequestType()));
        }
    }

    @Nonnull
    private PackageMessage createAdd(DistributionPackageBuilder packageBuilder, ResourceResolver resourceResolver, String pubAgentName, DistributionRequest request) throws DistributionException {
        final DistributionPackage disPkg = requireNonNull(packageBuilder.createPackage(resourceResolver, request));
        final byte[] pkgBinary = pkgBinary(disPkg);
        long pkgLength = assertPkgLength(pkgBinary.length);
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

        String storeRef;
        try {
            storeRef =  binaryStore.put(pkgId, disPkg.createInputStream(), pkgLength);
        } catch (IOException e) {
            throw new DistributionException(e.getMessage(), e);
        }

        if (StringUtils.isNotEmpty(storeRef)) {
            pkgBuilder.pkgBinaryRef(storeRef);
        } else {
            pkgBuilder.pkgBinary(pkgBinary);
        }
        PackageMessage pipePackage = pkgBuilder.build();
        
        disPkg.delete();
        return pipePackage;
    }

    @Nonnull
    private PackageMessage createInvalidate(DistributionPackageBuilder packageBuilder, ResourceResolver resourceResolver, DistributionRequest request, String pubAgentName) {
        String pkgId = UUID.randomUUID().toString();
        return PackageMessage.builder()
                .pubSlingId(pubSlingId)
                .pkgId(pkgId)
                .pubAgentName(pubAgentName)
                .paths(Arrays.asList(request.getPaths()))
                .reqType(ReqType.INVALIDATE)
                .pkgType(packageBuilder.getType())
                .userId(resourceResolver.getUserID())
                .build();
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

    private long assertPkgLength(long pkgLength) throws DistributionException {
        if (maxPackageSize >= 0 && pkgLength > maxPackageSize) {
            /*
             * To ensure that Apache Oak can import binary-less content packages
             * in an atomic save operation, we limit their supported size.
             */
            throw new DistributionException(format("Can't distribute package with size greater than %s Byte, actual %s", maxPackageSize, pkgLength));
        }
        return pkgLength;
    }

}
