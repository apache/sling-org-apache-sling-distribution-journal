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

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.Closeable;
import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JMXRegistration implements Closeable {
    public static final String DOMAIN = "org.apache.sling.distribution";

    private ObjectName name;
    
    public JMXRegistration(Object bean, String type, String id) {
        Hashtable<String, String> props = new Hashtable<>();
        props.put("type", type);
        props.put("id", id);
        try {
            this.name = ObjectName.getInstance(DOMAIN, props);
            MBeanServer server = getPlatformMBeanServer();
            if (!server.isRegistered(name)) {
                server.registerMBean(bean, name);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void close() {
        try {
            getPlatformMBeanServer().unregisterMBean(name);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
