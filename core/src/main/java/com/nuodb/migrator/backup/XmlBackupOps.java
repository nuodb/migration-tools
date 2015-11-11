/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.backup;

import com.nuodb.migrator.utils.xml.XmlHandlerRegistry;
import com.nuodb.migrator.utils.xml.XmlHandlerRegistryReader;
import com.nuodb.migrator.utils.xml.XmlHandlerStrategy;
import com.nuodb.migrator.utils.xml.XmlPersister;
import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.strategy.TreeStrategy;
import org.simpleframework.xml.stream.Format;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class XmlBackupOps extends BackupOpsBase implements XmlConstants {

    private final XmlPersister xmlPersister;

    public XmlBackupOps() {
        xmlPersister = createXmlPersister();
    }

    protected XmlPersister createXmlPersister() {
        return new XmlPersister(createXmlStrategy(), createFormat());
    }

    protected Strategy createXmlStrategy() {
        XmlHandlerRegistry xmlRegistry = new XmlHandlerRegistry();
        XmlHandlerRegistryReader registryReader = new XmlHandlerRegistryReader();
        registryReader.addRegistry(XML_HANDLER_REGISTRY);
        registryReader.read(xmlRegistry);
        return new XmlHandlerStrategy(xmlRegistry, new TreeStrategy());
    }

    protected Format createFormat() {
        return null;
    }

    @Override
    public Backup read(InputStream input, Map context) {
        return getXmlPersister().read(Backup.class, input, context);
    }

    @Override
    public void write(Backup backup, OutputStream output, Map context) {
        getXmlPersister().write(backup, output, context);
    }

    public XmlPersister getXmlPersister() {
        return xmlPersister;
    }
}
