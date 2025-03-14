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

import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JournalDistributionPackageTest {


    @Test
    public void testValues() {
        String id = randomUUID().toString();
        String type = randomUUID().toString();
        byte[] data = randomUUID().toString().getBytes();
        String[] paths = {"/some/paths"};
        String[] deepPaths = {"/some/paths"};
        Map<String, Object> props = new HashMap<>();
        props.put(PROPERTY_REQUEST_PATHS, paths);
        props.put(PROPERTY_REQUEST_DEEP_PATHS, deepPaths);
        DistributionPackageInfo pkgInfo = new DistributionPackageInfo(type, props);
        JournalDistributionPackage pkg = new JournalDistributionPackage(id, type, data, pkgInfo);
        assertEquals(id, pkg.getId());
        assertEquals(data.length, pkg.getSize());
        assertEquals(type, pkg.getType());
        assertEquals(pkgInfo, pkg.getInfo());
        assertEquals(type, pkg.getInfo().getType());
        assertArrayEquals(paths, pkg.getInfo().getPaths());
    }

}