package com.nuodb.migration.bootstrap.loader;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Set;

public class DynamicClassLoaderFactory {

    private static final Log log = LogFactory.getLog(DynamicClassLoaderFactory.class);

    public static DynamicClassLoader createClassLoader(Map<String, DynamicClassLoaderType> loaderTypes,
                                                       ClassLoader parent) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Creating dynamic class loader");
        }
        Set<URL> urls = Sets.newLinkedHashSet();
        if (loaderTypes != null) {
            for (Map.Entry<String, DynamicClassLoaderType> loaderType : loaderTypes.entrySet()) {
                String location = loaderType.getKey();
                DynamicClassLoaderType type = loaderType.getValue();
                switch (type) {
                    case URL:
                        URL url = new URL(location);
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Adding URL %1$s", url));
                        }
                        urls.add(url);
                        break;
                    case DIR:
                        File dir = new File(location);
                        dir = new File(dir.getCanonicalPath());
                        if (!dir.exists() || !dir.isDirectory() || !dir.canRead()) {
                            continue;
                        }
                        url = dir.toURI().toURL();
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Adding directory %1$s", url));
                        }
                        urls.add(url);
                        break;
                    case JAR:
                        File jar = new File(location);
                        jar = new File(jar.getCanonicalPath());
                        if (!jar.exists() || !jar.canRead()) {
                            continue;
                        }
                        url = jar.toURI().toURL();
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Adding JAR file %1$s", url));
                        }
                        urls.add(url);
                        break;
                    case JAR_DIR:
                        File jarDir = new File(location);
                        if (!jarDir.exists() || !jarDir.isDirectory() || !jarDir.canRead()) {
                            continue;
                        }
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Adding JAR directory %1$s", jarDir.getAbsolutePath()));
                        }
                        for (String fileName : jarDir.list()) {
                            if (!fileName.toLowerCase().endsWith(".jar")) {
                                continue;
                            }
                            File jarFile = new File(new File(jarDir, fileName).getCanonicalPath());
                            if (!jarFile.exists() || !jarFile.canRead()) {
                                continue;
                            }
                            if (log.isTraceEnabled()) {
                                log.trace(String.format("Adding JAR file %1$s", jarFile.getAbsolutePath()));
                            }
                            urls.add(jarFile.toURI().toURL());
                        }
                        break;
                }
            }
        }
        return parent == null ? new DynamicClassLoader(urls) : new DynamicClassLoader(urls, parent);
    }
}
