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
import static org.apache.jackrabbit.vault.fs.api.ProgressTrackerListener.Mode.PATHS;
import static org.apache.sling.api.resource.ResourceUtil.getOrCreateResource;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
import org.apache.jackrabbit.vault.packaging.PackageException;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.metrics.MetricsService;
import org.apache.sling.commons.metrics.internal.MetricsServiceImpl;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class ContentPackageExtractorTest {
    private static final boolean OVERWRITE_PRIMARY_TYPES_OF_FOLDERS = true;
    @Rule
    public final SlingContext scontext = new SlingContext(ResourceResolverType.JCR_MOCK);
    
    @Mock
    private Packaging packaging;
    
    @Mock
    private JcrPackageManager packageManager;

    @Mock
    private JcrPackage pkg;

    private OsgiContext context = new OsgiContext();
    private ResourceResolver resourceResolver;
	private SubscriberMetrics subscriberMetrics;

    @Before
    public void before() throws RepositoryException {
    	MetricsService metricsService = context.registerInjectActivateService(MetricsServiceImpl.class);
    	subscriberMetrics = new SubscriberMetrics(metricsService, "publish_subscriber", "publish", false);
        resourceResolver = scontext.resourceResolver();
        when(packaging.getPackageManager(Mockito.any(Session.class)))
        .thenReturn(packageManager);
        when(packageManager.open(Mockito.any(Node.class))).thenReturn(pkg);
    }

    @Test
    public void testNotPackagePath() throws Exception {
        Resource root = resourceResolver.getResource("/");
        Resource node = createNode(root, "other", NodeType.NT_FILE);
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Extract, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testNotFileNode() throws Exception {
        Resource packages = createEtcPackages();
        Resource node = createNode(packages, "mypackage", NodeType.NT_UNSTRUCTURED);
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Extract, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
    }

    
    @Test
    public void testNodeExtractNonExistantNode() throws Exception {
        Resource packages = createEtcPackages();
        
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Extract, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        
        // Should log a warning but not any exception
        extractor.handle(resourceResolver, singletonList(packages.getPath() + "/invalid"));
    }
    
    @Test
    public void testOff() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Off, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg, Mockito.never()).extract(Mockito.any(ImportOptions.class));
        verify(pkg, Mockito.never()).install(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testExtract() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Extract, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg).extract(Mockito.any(ImportOptions.class));
    }
    
    @Test
    public void testInstall() throws Exception {
        Resource node = createImportedPackage();

        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
        
        verify(pkg).install(Mockito.any(ImportOptions.class));
    }

    @Test(expected = DistributionException.class)
    public void testFailedInstall() throws Exception {

        doThrow(new PackageException()).when(pkg)
                .install(Mockito.any(ImportOptions.class));

        Resource node = createImportedPackage();
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
    }

    @Test(expected = DistributionException.class)
    public void testFailedInstallWithError() throws Exception {

        Answer<Void> errorAndThrow = run -> {
            ImportOptions opts = (ImportOptions) run.getArguments()[0];
            opts.getListener().onError(PATHS, "Failed due to XYZ", new PackageException());
            throw new PackageException();
        };

        doAnswer(errorAndThrow).when(pkg)
                .install(any(ImportOptions.class));

        Resource node = createImportedPackage();
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));
    }

    @Test
    public void testNotContentPackagePath() throws Exception {

        Resource packageRoot = getOrCreateResource(resourceResolver, "/tmp/packages", "package", "package", true);
        Resource node = createImportedPackage(packageRoot);

        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));

        verify(pkg, never()).install(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testNotContentPackage() throws Exception {
        Resource packageRoot = createEtcPackages();
        Resource node = createImportedPackage(packageRoot, NodeType.NT_FOLDER);

        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(node.getPath()));

        verify(pkg, never()).install(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testNullPath() throws Exception {
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList(null));
        verify(pkg, never()).install(Mockito.any(ImportOptions.class));
    }

    @Test
    public void testNullPackageNode() throws Exception {
        ContentPackageExtractor extractor = new ContentPackageExtractor(
                packaging, subscriberMetrics, PackageHandling.Install, OVERWRITE_PRIMARY_TYPES_OF_FOLDERS);
        extractor.handle(resourceResolver, singletonList("/does/not/exist"));
        verify(pkg, never()).install(Mockito.any(ImportOptions.class));
    }

    private Resource createImportedPackage() throws PersistenceException {
        return createImportedPackage(createEtcPackages());
    }

    private Resource createImportedPackage(Resource packageRoot) throws PersistenceException {
        return createImportedPackage(packageRoot, NodeType.NT_FILE);
    }

    private Resource createImportedPackage(Resource packageRoot, String packageNodeType) throws PersistenceException {
        Resource node1 = createNode(packageRoot, "my_packages", NodeType.NT_UNSTRUCTURED);
        return createNode(node1, "test-1.zip", packageNodeType);
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