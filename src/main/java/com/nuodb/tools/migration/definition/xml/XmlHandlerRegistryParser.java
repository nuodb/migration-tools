package com.nuodb.tools.migration.definition.xml;

import com.nuodb.tools.migration.utils.Assertions;
import com.nuodb.tools.migration.utils.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class XmlHandlerRegistryParser {

    protected transient final Log log = LogFactory.getLog(this.getClass());

    protected final List<URL> resources = new ArrayList<URL>();

    public void add(String resource) {
        add(ClassUtils.getClassLoader().getResource(resource));
    }

    public void add(URL resource) {
        Assertions.assertNotNull(resource, "Handler registry resource is required");
        resources.add(resource);
    }

    public void parse(XmlHandlerRegistry registry) {
        for (URL resource : resources) {
            try {
                parse(registry, resource.openStream());
            } catch (IOException e) {
                throw new XmlPersisterException(String.format("Failed reading handler registry %1$s", resource));
            }
        }
    }

    protected void parse(XmlHandlerRegistry registry, InputStream input) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String value;
        try {
            while ((value = reader.readLine()) != null) {
                parse(registry, value);
            }
        } catch (IOException e) {
            throw new XmlPersisterException("Failed loading registry", e);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed closing input stream", e);
                }
            }
        }
    }

    protected void parse(XmlHandlerRegistry registry, String value) {
        value = value.trim();
        int comma = value.lastIndexOf(",");
        String handlerClassAsText;
        int priority = XmlHandlerRegistry.PRIORITY_NORMAL;
        if (comma == -1) {
            handlerClassAsText = value;
        } else {
            handlerClassAsText = value.substring(0, comma);
            String priorityAsText = value.substring(comma + 1).trim();
            if (priorityAsText.length() != 0) {
                priority = Integer.parseInt(priorityAsText);
            }
        }
        XmlHandler handler = ClassUtils.newInstance(handlerClassAsText);
        registry.register(handler, priority);
    }
}
