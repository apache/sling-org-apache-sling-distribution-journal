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
package org.apache.sling.distribution.journal.impl.subscriber;

import java.io.Closeable;
import java.util.Hashtable;

import org.apache.felix.systemready.CheckStatus;
import org.apache.felix.systemready.StateType;
import org.apache.felix.systemready.SystemReadyCheck;
import org.apache.felix.systemready.CheckStatus.State;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class SubscriberIdleCheck implements SystemReadyCheck, Closeable {
    private final ServiceRegistration<SystemReadyCheck> reg;
    private final IdleCheck idleCheck;
    
    public SubscriberIdleCheck(BundleContext context, IdleCheck idleCheck) {
        this.idleCheck = idleCheck;
        this.reg = context.registerService(SystemReadyCheck.class, this, new Hashtable<>());
    }

    @Override
    public String getName() {
        return "DistributionSubscriber idle";
    }

    @Override
    public CheckStatus getStatus() {
        State state = idleCheck.isIdle() ? State.GREEN : State.RED; 
        return new CheckStatus(getName(), StateType.READY, state, "DistributionSubscriber idle");
    }

    @Override
    public void close() {
        if (reg != null) {
            reg.unregister();
        }
    }

}
