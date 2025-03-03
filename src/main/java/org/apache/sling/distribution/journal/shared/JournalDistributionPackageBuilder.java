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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.jackrabbit.vault.fs.api.ImportMode;
import org.apache.jackrabbit.vault.fs.io.AccessControlHandling;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.apache.sling.distribution.serialization.DistributionContentSerializer;
import org.apache.sling.distribution.serialization.DistributionExportOptions;
import org.apache.sling.distribution.serialization.impl.vlt.FileVaultContentSerializer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.io.IOUtils.toByteArray;
import static org.apache.sling.distribution.journal.shared.JournalDistributionPackage.PROPERTY_BINARY_STORE_REF;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.*;
import static org.apache.sling.distribution.packaging.impl.DistributionPackageUtils.fillInfo;
import static org.slf4j.LoggerFactory.getLogger;

@Component(service = DistributionPackageBuilder.class)
@Designate(ocd = JournalDistributionPackageBuilder.Configuration.class, factory = true)
public class JournalDistributionPackageBuilder implements DistributionPackageBuilder {

    private static final Logger LOG = getLogger(JournalDistributionPackageBuilder.class);

    private final String type;

    private final DistributionContentSerializer contentSerializer;

    private final BinaryStore binaryStore;

    @Activate
    public JournalDistributionPackageBuilder(
            Configuration config,
            @Reference Packaging packaging,
            @Reference BinaryStore binaryStore) {
        this.binaryStore = requireNonNull(binaryStore);
        type = config.name();
        contentSerializer = new FileVaultContentSerializer(
                config.name(),
                packaging,
                ImportMode.valueOf(config.importMode()),
                AccessControlHandling.valueOf(config.aclHandling()),
                AccessControlHandling.valueOf(config.cugHandling()),
                config.package_roots(),
                config.package_filters(),
                config.property_filters(),
                config.useBinaryReferences(),
                config.autoSaveThreshold(),
                pathMappings(config.pathsMapping()),
                config.strictImport());
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public @Nonnull DistributionPackage createPackage(@Nonnull ResourceResolver resourceResolver,
                                                      @Nonnull DistributionRequest distributionRequest)
            throws DistributionException {

        String packageId = format("dstrpck-%s-%s", currentTimeMillis(), randomUUID());

        final byte[] data;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DistributionExportOptions distributionExportOptions = new DistributionExportOptions(distributionRequest, null /* Filters set on the serializer */);
            contentSerializer.exportToStream(resourceResolver, distributionExportOptions, outputStream);
            data = outputStream.toByteArray();
        } catch (IOException e) {
            throw new DistributionException("Failed to create package for paths: " + Arrays.toString(distributionRequest.getPaths()), e);
        }

        final String storeRef;
        try (InputStream inputStream = new ByteArrayInputStream(data)) {
            storeRef = binaryStore.put(packageId, inputStream, data.length);
        } catch (IOException e) {
            throw new DistributionException("Failed to upload package to binary store", e);
        }

        DistributionPackageInfo distributionPackageInfo = new DistributionPackageInfo(getType());
        fillInfo(distributionPackageInfo, distributionRequest);
        distributionPackageInfo.put(PROPERTY_BINARY_STORE_REF, storeRef);

        return new JournalDistributionPackage(packageId, getType(), data, distributionPackageInfo);
    }

    @Nonnull
    @Override
    public DistributionPackage readPackage(@Nonnull ResourceResolver resourceResolver,
                                           @Nonnull InputStream inputStream)
            throws DistributionException {
        ObjectReader reader = new ObjectMapper().readerFor(PackageMessage.class);
        final PackageMessage pkgMsg;
        try {
            pkgMsg = reader.readValue(inputStream);
        } catch (IOException e) {
            throw new DistributionException("Failed to deserialize package message", e);
        }
        final byte[] data;
        try {
            data = toByteArray(binaryStore.get(pkgMsg.getPkgBinaryRef()));
        } catch (IOException e) {
            throw new DistributionException("Failed to download package from binary store", e);
        }
        DistributionPackageInfo distributionPackageInfo = new DistributionPackageInfo(pkgMsg.getPkgType());
        distributionPackageInfo.put(PROPERTY_REQUEST_PATHS, pkgMsg.getPaths().toArray());
        distributionPackageInfo.put(PROPERTY_REQUEST_DEEP_PATHS, pkgMsg.getDeepPaths().toArray());
        distributionPackageInfo.put(PROPERTY_REQUEST_TYPE, pkgMsg.getReqType());
        return new JournalDistributionPackage(pkgMsg.getPkgId(), pkgMsg.getPkgType(), data, distributionPackageInfo);
    }

    @Nullable
    @Override
    public DistributionPackage getPackage(@Nonnull ResourceResolver resourceResolver,
                                          @Nonnull String id)
            throws DistributionException {
        throw new DistributionException("Unsupported Operation with id: " + id);
    }

    @Override
    public boolean installPackage(@Nonnull ResourceResolver resourceResolver,
                                  @Nonnull DistributionPackage distributionPackage)
            throws DistributionException {
        DistributionPackageInfo info = distributionPackage.getInfo();
        DistributionRequestType requestType = requireNonNull(info.getRequestType());
        switch (requestType) {
            case ADD: installAddPackage(resourceResolver, distributionPackage); break;
            case DELETE: installDeletePackage(resourceResolver, distributionPackage); break;
            default: LOG.debug("Skip request type: {}", requestType);
        }
        return true;
    }

    @Override
    public @Nonnull DistributionPackageInfo installPackage(@Nonnull ResourceResolver resourceResolver,
                                                           @Nonnull InputStream inputStream)
            throws DistributionException {
        throw new DistributionException("Unsupported Operation");
    }

    protected Map<String, String> pathMappings(String[] pathMappings) {
        return unmodifiableMap(Arrays.stream(pathMappings)
                .map(mapping -> mapping.split("=", 2))
                .filter(chunks -> chunks.length == 2)
                .collect(toMap(chunks -> chunks[0], chunks -> chunks[1], (existing, replacement) -> replacement)));
    }

    private void installAddPackage(@Nonnull ResourceResolver resourceResolver,
                                   @Nonnull DistributionPackage distributionPackage)
            throws DistributionException {
        try (InputStream inputStream = distributionPackage.createInputStream()) {
            contentSerializer.importFromStream(resourceResolver, inputStream);
        } catch (IOException e) {
            throw new DistributionException("Failed to install distribution package with id: " + distributionPackage.getId(), e);
        }
    }

    private void installDeletePackage(@Nonnull ResourceResolver resourceResolver,
                                      @Nonnull DistributionPackage distributionPackage)
            throws DistributionException {
        List<String> paths = asList(distributionPackage.getInfo().getPaths());
        LOG.info("Deleting paths {}", paths);
        for (String path : paths) {
            Resource resource = resourceResolver.getResource(path);
            if (resource != null) {
                try {
                    resourceResolver.delete(resource);
                } catch (PersistenceException e) {
                    throw new DistributionException(e);
                }
            }
        }
    }

    @ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Package Builder Configuration",
            description = "Apache Sling Content Distribution Package Builder Configuration")
    public @interface Configuration {

        @AttributeDefinition
        String webconsole_configurationFactory_nameHint() default "Builder name: {name}";

        @AttributeDefinition(name = "Name",
            description = "The name of the package builder.")
        String name() default "journal-distribution";

        @AttributeDefinition(name = "Import Mode",
                description = "The vlt import mode for created packages.")
        String importMode() default "REPLACE";

        @AttributeDefinition(name = "Acl Handling",
            description = "The vlt acl handling mode for created packages.")
        String aclHandling() default "MERGE_PRESERVE";

        @AttributeDefinition(name = "Cug Handling",
            description = "The vlt cug handling mode for created packages.")
        String cugHandling() default "OVERWRITE";

        @AttributeDefinition(name = "Package Roots",
                description = "The package roots to be used for created packages. (this is useful for assembling packages with an user that cannot read above the package root)")
        String[] package_roots() default {};

        @AttributeDefinition(name = "Package Node Filters",
                description = "The package node path filters. Filter format: path|+include|-exclude")
        String[] package_filters() default {};

        @AttributeDefinition(name = "Package Property Filters",
                description = "The package property path filters. Filter format: path|+include|-exclude")
        String[] property_filters() default {};

        @AttributeDefinition(name = "Use Binary References",
                description = "If activated, it avoids sending binaries in the distribution package.")
        boolean useBinaryReferences() default true;

        @AttributeDefinition(name = "Autosave threshold",
                description = "The value after which autosave is triggered for intermediate changes.")
        int autoSaveThreshold() default 1000;

        @AttributeDefinition(name = "Paths mapping",
                description = "List of paths that require be mapped.The format is {sourcePattern}={destinationPattern}, e.g. /etc/(.*)=/var/$1/some or simply /data=/bak")
        String[] pathsMapping() default {};


        @AttributeDefinition(name = "Install a content package in a strict mode",
                description = "Flag to mark an error response will be thrown, if a content package will incorrectly installed")
        boolean strictImport() default true;
    }
}
