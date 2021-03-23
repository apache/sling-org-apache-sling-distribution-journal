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

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

import java.io.IOException;
import java.util.Objects;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.apache.sling.distribution.journal.shared.LocalStoreJMXMBean;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
    immediate = true
)
public class LocalStoreJMX implements LocalStoreJMXMBean {
    
    Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Reference
    private ResourceResolverFactory resolverFactory;
    
    private JMXRegistration jmxRegistration;

    @Activate
    public void activate() throws NotCompliantMBeanException {
        Objects.requireNonNull(resolverFactory, "resolverFactory must not be null");
        StandardMBean mbean = new StandardMBean(this, LocalStoreJMXMBean.class);
        jmxRegistration = new JMXRegistration(mbean, "offsetReset", "default");
    }
    
    @Deactivate
    public void close() throws IOException {
        jmxRegistration.close();
    }

    @Override
    public void resetStores() {
        String path = LocalStore.ROOT_PATH;
        try (ResourceResolver resolver = getBookKeeperServiceResolver()) {
            Resource rootResource = Objects.requireNonNull(resolver.getResource(path), path + " not found");
            // We must not delete the root Resource as it is provisioned by the feature. So we delete the children instead
            Iterable<Resource> children = resolver.getChildren(rootResource);
            children.forEach(res -> delete(resolver, res));
            resolver.commit();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    private void delete(ResourceResolver resolver, Resource resource) {
        try {
            log.info("Deleting store {}", resource.getPath());
            resolver.delete(resource);
        } catch (PersistenceException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    private ResourceResolver getBookKeeperServiceResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"));
    }
}
