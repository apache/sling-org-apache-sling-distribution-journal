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

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.vault.fs.io.ImportOptions;
import org.apache.jackrabbit.vault.packaging.JcrPackage;
import org.apache.jackrabbit.vault.packaging.JcrPackageManager;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;

@RunWith(MockitoJUnitRunner.class)
public class ContentPackageExtractorTest {
    
    @Mock
    private Packaging packaging;
    
    @Mock
    private JcrPackageManager packageManager;

    @Mock
    private JcrPackage pkg;

    private ResourceResolver resourceResolver;

    @Before
    public void before() throws RepositoryException {
        BundleContext context = MockOsgi.newBundleContext();
        resourceResolver = MockSling.newResourceResolver(ResourceResolverType.JCR_MOCK, context);
        when(packaging.getPackageManager(Mockito.any(Session.class)))
        .thenReturn(packageManager);
        when(packageManager.open(Mockito.any(Node.class))).thenReturn(pkg);
    }

    @Test
    public void testNotPackagePath() throws Exception {
        Resource root = resourceResolver.getResource("/");
        Resource node = createNode(root, "other", NodeType.NT_FILE);
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Extract);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testNotFileNode() throws Exception {
        Resource packages = createEtcPackages();
        Resource node = createNode(packages, "mypackage", NodeType.NT_UNSTRUCTURED);
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Extract); 
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
    }

    
    @Test
    public void testNodeExtractNonExistantNode() throws Exception {
        Resource packages = createEtcPackages();
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Extract);
        
        // Should log a warning but not any exception
        extractor.handle(resourceResolver, singletonList(packages.getPath() + "/invalid"));
    }
    
    @Test
    public void testOff() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Off); 
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
        verify(pkg, Mockito.never()).install(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testExtract() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Extract); 
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg).extract(Mockito.any(ImportOptions.class));
    }
    
    @Test
    public void testInstall() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(packaging, PackageHandling.Install); 
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg).install(Mockito.any(ImportOptions.class));
    }


    private Resource createImportedPackage() throws PersistenceException {
        Resource packages = createEtcPackages();
        Resource node1 = createNode(packages, "my_packages", NodeType.NT_UNSTRUCTURED);
        return createNode(node1, "test-1.zip", NodeType.NT_FILE);
    }

    private Resource createNode(Resource parent, String name, String nodeType) throws PersistenceException {
        Map<String, Object> props = new HashMap<>();
        props.put("jcr:primaryType", nodeType);
        return resourceResolver.create(parent, name, props);
    }
    
    private Resource createEtcPackages() throws PersistenceException {
        Map<String, Object> props = new HashMap<>();
        Resource root = resourceResolver.getResource("/");
        Resource etc = resourceResolver.create(root, "etc", props);
        return resourceResolver.create(etc, "packages", props);
    }

}