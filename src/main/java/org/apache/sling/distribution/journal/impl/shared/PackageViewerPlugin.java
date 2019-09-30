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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.felix.webconsole.AbstractWebConsolePlugin;
import org.apache.felix.webconsole.WebConsoleConstants;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.queue.impl.RangePoller;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage.ReqType;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

@Component(service = Servlet.class,
        property = {
        Constants.SERVICE_DESCRIPTION + "=" + "Package viewer for content distribution",
        WebConsoleConstants.PLUGIN_LABEL + "=" + PackageViewerPlugin.LABEL,
        WebConsoleConstants.PLUGIN_TITLE + "=" + PackageViewerPlugin.TITLE
})
public class PackageViewerPlugin extends AbstractWebConsolePlugin {
    private static final int NOT_FOUND = 404;
    private static final int TIMEOUT = 1000;
    private static final int MAX_NUM_MESSAGES = 100;
    private static final long serialVersionUID = -3113699912185558439L;
    protected static final String LABEL = "distpackages";
    protected static final String TITLE = "Distribution Package Viewer";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Reference
    private JournalAvailable journalAvailable;
    
    @Reference
    private MessagingProvider messagingProvider;
    
    @Reference
    private Topics topics;

    @Override
    public String getLabel() {
        return LABEL;
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public String getCategory() {
        return "Sling";
    }
    
    @Override
    protected void renderContent(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        Optional<Long> offset = getOffset(req);
        if (!offset.isPresent()) {
            String startOffsetSt = req.getParameter("startOffset");
            long startOffset = startOffsetSt != null ? new Long(startOffsetSt) : 0;
            renderPackageList(startOffset, res.getWriter());
        } else {
            writePackage(offset.get(), res);
        }
    }

    private void renderPackageList(long startOffset, PrintWriter writer) {
        writer.println("<table class=\"tablesorter nicetable noauto\">");
        writer.println("<tr><th>Id</th><th>Offset</th><th>Type</th><th>Paths</th></tr>");
        List<FullMessage<PackageMessage>> msgs = getMessages(startOffset, Integer.MAX_VALUE);
        msgs.stream().filter(this::notTestMessage).map(this::writeMsg).forEach(writer::println);
        writer.println("</table>");
    }

    private String writeMsg(FullMessage<PackageMessage> msg) {
        return String.format("<tr><td><a href=\"%s/%d\">%s</a></td><td>%d</td><td>%s</td><td>%s</td></tr>",
                LABEL,
                msg.getInfo().getOffset(),
                msg.getMessage().getPkgId(),
                msg.getInfo().getOffset(),
                msg.getMessage().getReqType(),
                msg.getMessage().getPathsList().toString());
    }

    private void writePackage(Long offset, HttpServletResponse res) throws IOException {
        log.info("Retrieving package with offset " + offset);
        Optional<PackageMessage> msg = getPackage(offset);
        if (msg.isPresent()) {
            res.setHeader("Content-Type", "application/octet-stream");
            String filename = msg.get().getPkgId() + ".zip";
            res.setHeader("Content-Disposition" , "inline; filename=\"" + filename + "\"");
            msg.get().getPkgBinary().writeTo(res.getOutputStream());
        } else {
            res.setStatus(NOT_FOUND);
        }
    }
    
    @Override
    protected boolean isHtmlRequest(HttpServletRequest request) {
        return !getOffset(request).isPresent();
    }

    private Optional<Long> getOffset(HttpServletRequest req) {
        int startIndex = LABEL.length() + 2;
        if (startIndex <= req.getPathInfo().length()) {
            String offsetSt = req.getPathInfo().substring(startIndex);
            return Optional.of(new Long(offsetSt));
        } else {
            return Optional.absent();
        }
    }

    private boolean notTestMessage(FullMessage<PackageMessage> msg) {
        return msg.getMessage().getReqType() != ReqType.TEST;
    }
    
    private Optional<PackageMessage> getPackage(long offset) {
        List<FullMessage<PackageMessage>> messages = getMessages(offset, offset + 1);
        if (messages.isEmpty()) {
            return Optional.absent();
        } else {
            FullMessage<PackageMessage> fullMsg = messages.iterator().next();
            PackageMessage msg = fullMsg.getMessage();
            log.info("Retrieved package with id: {}, offset: {}, type: {}, paths: {}",
                    msg.getPkgId(), fullMsg.getInfo().getOffset(), msg.getReqType(),
                    msg.getPathsList().toString());
            return Optional.of(fullMsg.getMessage());
        }
    }

    protected List<FullMessage<PackageMessage>> getMessages(long startOffset, long endOffset) {
        try {
            RangePoller poller = new RangePoller(messagingProvider, topics.getPackageTopic(), startOffset, endOffset);
            return poller.fetchRange(MAX_NUM_MESSAGES, TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}