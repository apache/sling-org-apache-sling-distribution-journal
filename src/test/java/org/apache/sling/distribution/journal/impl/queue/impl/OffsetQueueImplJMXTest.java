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
package org.apache.sling.distribution.journal.impl.queue.impl;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.sling.distribution.journal.impl.queue.OffsetQueue;
import org.apache.sling.distribution.journal.shared.JMXRegistration;
import org.junit.Test;

public class OffsetQueueImplJMXTest {

    @Test
    public void testJMX() throws MalformedObjectNameException, InstanceNotFoundException, AttributeNotFoundException, ReflectionException, MBeanException {
        OffsetQueue<Long> queue = new OffsetQueueImpl<>();
        queue.putItem(100, 100L);
        queue.putItem(105, 105L);

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        JMXRegistration jmxReg = new JMXRegistration(queue, OffsetQueue.class.getSimpleName(), "test");

        Set<ObjectInstance> mbeans = mbeanServer.queryMBeans(new ObjectName("org.apache.sling.distribution:type=OffsetQueue,id=test"), null);
        ObjectInstance mbean = mbeans.iterator().next();
        assertThat(getAttrib(mbean, "Size"), equalTo(2));
        assertThat(getAttrib(mbean, "HeadOffset"), equalTo(100L));
        assertThat(getAttrib(mbean, "TailOffset"), equalTo(105L));

        jmxReg.close();
    }

    private Object getAttrib(ObjectInstance mbean, String key)
            throws InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        return mbeanServer.getAttribute(mbean.getObjectName(), key);
    }
}