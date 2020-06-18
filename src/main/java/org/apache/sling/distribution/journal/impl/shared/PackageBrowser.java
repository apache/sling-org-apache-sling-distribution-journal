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
package org.apache.sling.distribution.journal.impl.shared;

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = PackageBrowser.class)
public class PackageBrowser {
    
    @Reference
    private JournalAvailable journalAvailable;
    
    @Reference
    private MessagingProvider messagingProvider;
    
    @Reference
    private Topics topics;
    
    @Reference
    ResourceResolverFactory resolverFactory;
    
    public List<FullMessage<PackageMessage>> getMessages(long startOffset, long numMessages, Duration timeout) {
        LimitPoller poller = new LimitPoller(messagingProvider, topics.getPackageTopic(), startOffset, numMessages);
        return poller.fetch(timeout);
    }
    
    public void writeTo(PackageMessage pkgMsg, OutputStream os) throws IOException {
        try (ResourceResolver resolver = getServiceResolver("importer")) {
            InputStream is = pkgStream(resolver, pkgMsg);
            IOUtils.copy(is, os);
        } catch (LoginException | DistributionException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
    
    @Nonnull
    public static InputStream pkgStream(ResourceResolver resolver, PackageMessage pkgMsg) throws DistributionException {
        if (pkgMsg.getPkgBinary() != null) {
            return new ByteArrayInputStream(pkgMsg.getPkgBinary());
        } else {
            String pkgBinRef = pkgMsg.getPkgBinaryRef();
            try {
                Session session = resolver.adaptTo(Session.class);
                if (session == null) {
                    throw new DistributionException("Unable to get Oak session");
                }
                ValueFactory factory = session.getValueFactory();
                Binary binary = factory.createValue(new SimpleReferenceBinary(pkgBinRef)).getBinary();
                return binary.getStream();
            } catch (RepositoryException e) {
                throw new DistributionException(e.getMessage(), e);
            }
        }
    }
    
    private ResourceResolver getServiceResolver(String subService) throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, subService));
    }
}
