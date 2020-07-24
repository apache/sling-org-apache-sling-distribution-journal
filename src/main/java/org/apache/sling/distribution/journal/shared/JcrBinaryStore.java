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

import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Binary;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.impl.publisher.PackageRepo;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

@Component(
    property = {
        "type=jcr",
        "service.ranking:Integer=10"
    }
)
public class JcrBinaryStore implements BinaryStore {

    private static final Logger LOG = LoggerFactory.getLogger(JcrBinaryStore.class);

    private static final long MAX_INLINE_PKG_BINARY_SIZE = 800L * 1024;

    @Reference
    private PackageRepo packageRepo;

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Override public InputStream get(String reference) throws IOException {
        try (ResourceResolver resolver = createResourceResolver()) {
            Session session = resolver.adaptTo(Session.class);
            if (session == null) {
                throw new IOException("Unable to get Oak session");
            }
            ValueFactory factory = session.getValueFactory();
            Binary binary = factory.createValue(new SimpleReferenceBinary(reference)).getBinary();
            return binary.getStream();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public String put(String id, InputStream stream, long length) throws IOException {
        if (length > MAX_INLINE_PKG_BINARY_SIZE) {

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

            LOG.info("Package {} too large ({}B) to be sent inline", id, length);
            try {
                return packageRepo.store(id, stream);
            } catch (DistributionException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
        return null;
    }

    private ResourceResolver createResourceResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"));
    }
}
