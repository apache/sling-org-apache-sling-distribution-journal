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
package org.apache.sling.distribution.journal;

import java.lang.reflect.Method;

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistryLite;

public class HandlerAdapter<T> {
    private final MessageHandler<T> handler;
    private final Method method;
    private final ExtensionRegistryLite registry;
    private final Class<T> type;
    
    public HandlerAdapter(Class<T> type, MessageHandler<T> handler) {
        this.type = type;
        this.handler = handler;
        try {
            method = type.getMethod("parseFrom", ByteString.class, ExtensionRegistryLite.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        registry = ExtensionRegistryLite.newInstance();
    }

    public static <T> HandlerAdapter<T> create(Class<T> type, MessageHandler<T> handler) {
        return new HandlerAdapter<>(type, handler);
    }
    
    @SuppressWarnings("unchecked")
    private T parseFrom(ByteString payloadBytes) throws Exception {
        return (T) method.invoke(null, payloadBytes, registry);
    }
    
    public void handle(MessageInfo info, ByteString payloadBytes) throws Exception {
        T payload = parseFrom(payloadBytes);
        handler.handle(info, payload);
    }

    public Class<?> getType() {
        return type;
    }

    public MessageHandler<T> getHandler() {
        return this.handler;
    }
}
