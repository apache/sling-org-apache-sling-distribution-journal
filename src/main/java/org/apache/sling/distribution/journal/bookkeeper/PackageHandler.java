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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.sling.distribution.journal.messages.PackageMessage.ReqType.ADD;

class PackageHandler {

    private final DistributionPackageBuilder packageBuilder;

    private final ContentPackageExtractor extractor;

    public PackageHandler(DistributionPackageBuilder packageBuilder, ContentPackageExtractor extractor) {
        this.packageBuilder = packageBuilder;
        this.extractor = extractor;
    }

    public void apply(ResourceResolver resolver, PackageMessage pkgMsg)
            throws DistributionException, PersistenceException {
        DistributionPackage distributionPackage = packageBuilder
                .readPackage(resolver, serialise(pkgMsg));
        packageBuilder.installPackage(resolver, distributionPackage);
        ReqType type = pkgMsg.getReqType();
        if (type == ADD) {
            extractor.handle(resolver, pkgMsg.getPaths());
        }
    }

    private InputStream serialise(PackageMessage pkgMsg)
            throws DistributionException {
        ObjectWriter writer = new ObjectMapper().writerFor(PackageMessage.class);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            writer.writeValue(outputStream, pkgMsg);
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (IOException e) {
            throw new DistributionException(e);
        }
    }
}
