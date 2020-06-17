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
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.felix.webconsole.AbstractWebConsolePlugin;
import org.apache.felix.webconsole.WebConsoleConstants;
import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = Servlet.class,
        property = {
        Constants.SERVICE_DESCRIPTION + "=" + "Package viewer for content distribution",
        WebConsoleConstants.PLUGIN_LABEL + "=" + PackageViewerPlugin.LABEL,
        WebConsoleConstants.PLUGIN_TITLE + "=" + PackageViewerPlugin.TITLE
})
public class PackageViewerPlugin extends AbstractWebConsolePlugin {
    private static final Duration BROWSE_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration DOWNLOAD_TIMEOUT = Duration.ofMillis(20000);
    private static final int NOT_FOUND = 404;
    private static final int MAX_NUM_MESSAGES = 100;
    private static final long serialVersionUID = -3113699912185558439L;
    protected static final String LABEL = "distpackages";
    protected static final String TITLE = "Distribution Package Viewer";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Reference
    PackageBrowser packageBrowser; //NOSONAR
    
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
            throws IOException {
        Optional<Long> offset = getOffset(req);
        if (!offset.isPresent()) {
            String startOffsetSt = req.getParameter("startOffset");
            long startOffset = startOffsetSt != null ? Long.parseLong(startOffsetSt) : 0;
            renderPackageList(startOffset, res.getWriter());
        } else {
            writePackage(offset.get(), res);
        }
    }

    private void renderPackageList(long startOffset, PrintWriter writer) {
        writer.println("<table class=\"tablesorter nicetable noauto\">");
        writer.println("<tr><th>Id</th><th>Offset</th><th>Type</th><th>Paths</th></tr>");
        List<FullMessage<PackageMessage>> msgs = packageBrowser.getMessages(startOffset, MAX_NUM_MESSAGES, BROWSE_TIMEOUT);
        msgs.stream().filter(this::notTestMessage).map(this::writeMsg).forEach(writer::println);
        writer.println("</table>");
        if (msgs.size() == MAX_NUM_MESSAGES) {
            FullMessage<PackageMessage> lastMsg = msgs.get(msgs.size() - 1);
            long nextOffset = lastMsg.getInfo().getOffset() + 1;
            writer.println(String.format("<p><a href =\"%s?startOffset=%d\">next page</a>",
                    LABEL,
                    nextOffset));
        }
    }

    private String writeMsg(FullMessage<PackageMessage> msg) {
        return String.format("<tr><td><a href=\"%s/%d\">%s</a></td><td>%d</td><td>%s</td><td>%s</td></tr>",
                LABEL,
                msg.getInfo().getOffset(),
                msg.getMessage().getPkgId(),
                msg.getInfo().getOffset(),
                msg.getMessage().getReqType(),
                msg.getMessage().getPaths().toString());
    }

    private void writePackage(Long offset, HttpServletResponse res) throws IOException {
        log.info("Retrieving package with offset {}", offset);
        List<FullMessage<PackageMessage>> msgs = packageBrowser.getMessages(offset, 1, DOWNLOAD_TIMEOUT);
        if (!msgs.isEmpty()) {
            PackageMessage msg = msgs.iterator().next().getMessage();
            res.setHeader("Content-Type", "application/octet-stream");
            String filename = msg.getPkgId() + ".zip";
            res.setHeader("Content-Disposition" , "inline; filename=\"" + filename + "\"");
            packageBrowser.writeTo(msg, res.getOutputStream());
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
            return Optional.of(Long.valueOf(offsetSt));
        } else {
            return Optional.empty();
        }
    }

    private boolean notTestMessage(FullMessage<PackageMessage> msg) {
        return msg.getMessage().getReqType() != ReqType.TEST;
    }
    

}