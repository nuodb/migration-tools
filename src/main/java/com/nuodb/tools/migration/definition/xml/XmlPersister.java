package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.stream.Format;

import java.io.InputStream;
import java.io.OutputStream;

public class XmlPersister {

    private static final String PROLOG = "<?xml version=\"1.0\"?>";

    private Persister persister;

    public XmlPersister(XmlHandlerRegistry registry) {
        this(new XmlHandlerStrategy(registry));
    }

    public XmlPersister(XmlHandlerStrategy strategy) {
        this(strategy, new Format(PROLOG));
    }

    public XmlPersister(XmlHandlerStrategy strategy, Format format) {
        this.persister = new Persister(strategy, format);
    }

    public <T> T read(Class<T> type, InputStream input) {
        try {
            return persister.read(type, input);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    public void write(Object source, OutputStream output) {
        try {
            persister.write(source, output);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }
}
