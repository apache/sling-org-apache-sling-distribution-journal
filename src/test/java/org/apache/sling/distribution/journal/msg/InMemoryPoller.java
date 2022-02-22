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
package org.apache.sling.distribution.journal.msg;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.Reset;

public class InMemoryPoller implements Closeable {

    private InMemoryTopic topic;
    private Adapter<?> adapter;
    private AtomicBoolean closed = new AtomicBoolean();
    private ExecutorService executor;
    private long curOffset;

    public InMemoryPoller(InMemoryTopic topic, Reset reset, String assign, HandlerAdapter<?>[] adapters) {
        adapter = new Adapter<>(adapters[0]);
        this.topic = topic;
        curOffset = this.topic.getOffset(reset, assign);
        executor = Executors.newSingleThreadExecutor();
        executor.execute(this::run);
    }

    @Override
    public void close() throws IOException {
        this.closed.set(true);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void run() {
        while (!closed.get()) {
            FullMessage<?> fullMsg = topic.next(curOffset);
            if (fullMsg != null) {
                Object msg = fullMsg.getMessage();
                MessageInfo info = fullMsg.getInfo();
                try {
                    adapter.handle(fullMsg.getInfo(), msg);
                    this.curOffset = info.getOffset();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    class Adapter<T> {
        final MessageHandler<T> handler;
        final Class<?> type;

        public Adapter(HandlerAdapter<T> handler) {
            this.type = handler.getType();
            this.handler = handler.getHandler();
        }

        void handle(MessageInfo info, Object msg) {
            @SuppressWarnings("unchecked")
            T parsed = (T) msg;
            handler.handle(info, parsed);
        }
    }
}
