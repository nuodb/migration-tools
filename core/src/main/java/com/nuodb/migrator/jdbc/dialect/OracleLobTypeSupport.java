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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.type.JdbcTypeException;
import com.nuodb.migrator.jdbc.type.adapter.JdbcLobTypeSupport;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class OracleLobTypeSupport implements JdbcLobTypeSupport {

    private static final String BLOB_CLASS_NAME = "oracle.sql.BLOB";
    private static final String CLOB_CLASS_NAME = "oracle.sql.CLOB";
    private static final String DURATION_SESSION_FIELD_NAME = "DURATION_SESSION";
    private static final String MODE_READWRITE_FIELD_NAME = "MODE_READWRITE";
    private static final String MODE_READONLY_FIELD_NAME = "MODE_READONLY";

    private final Map<Class, Integer> durationSessionConstants = newHashMap();
    private final Map<Class, Integer> modeReadWriteConstants = newHashMap();
    private final Map<Class, Integer> modeReadOnlyConstants = newHashMap();

    private Class<?> blobClass;
    private Class<?> clobClass;
    private boolean initLobClasses;
    private boolean releaseLobAfterAccess = true;
    private boolean cache = true;

    @Override
    public Clob createClob(Connection connection) {
        return createLob(connection, clobClass);
    }

    @Override
    public Blob createBlob(Connection connection) {
        return createLob(connection, blobClass);
    }

    @Override
    public void closeClob(Connection connection, Clob clob) {
        closeLob(connection, clob);
    }

    @Override
    public void closeBlob(Connection connection, Blob clob) {
        closeLob(connection, clob);
    }

    @Override
    public void initClobBeforeAccess(Connection connection, Clob clob) {
        initLobBeforeAccess(connection, clob);
    }

    @Override
    public void initBlobBeforeAccess(Connection connection, Blob blob) {
        initLobBeforeAccess(connection, blob);
    }

    @Override
    public void releaseClobAfterAccess(Connection connection, Clob clob) {
        releaseLobAfterAccess(connection, clob);
    }

    @Override
    public void releaseBlobAfterAccess(Connection connection, Blob blob) {
        releaseLobAfterAccess(connection, blob);
    }

    private void closeLob(Connection connection, Object lob) {
        try {
            lob.getClass().getMethod("close", (Class[]) null).invoke(lob, (Object[]) null);
        } catch (InvocationTargetException exception) {
            throw new JdbcTypeException(exception.getTargetException());
        } catch (IllegalAccessException exception) {
            throw new JdbcTypeException(exception);
        } catch (NoSuchMethodException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    private <T> T createLob(Connection connection, Class<?> lobClass) {
        initLobClasses(connection);
        try {
            Method createTemporary = lobClass.getMethod("createTemporary", Connection.class, boolean.class, int.class);
            Object lob = createTemporary.invoke(null, connection, isCache(), durationSessionConstants.get(lobClass));
            Method open = lobClass.getMethod("open", int.class);
            return (T) open.invoke(lob, modeReadWriteConstants.get(lobClass));
        } catch (InvocationTargetException exception) {
            throw new JdbcTypeException(exception.getTargetException());
        } catch (NoSuchMethodException exception) {
            throw new JdbcTypeException(exception);
        } catch (IllegalAccessException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    private void initLobBeforeAccess(Connection connection, Object lob) {
        if (isReleaseLobAfterAccess()) {
            initLobClasses(connection);
            try {
                Method isTemporary = lob.getClass().getMethod("isTemporary");
                Boolean temporary = (Boolean) isTemporary.invoke(lob);
                if (!temporary) {
                    Method open = lob.getClass().getMethod("open", int.class);
                    open.invoke(lob, modeReadOnlyConstants.get(lob.getClass()));
                }
            } catch (InvocationTargetException exception) {
                throw new JdbcTypeException(exception.getTargetException());
            } catch (NoSuchMethodException exception) {
                throw new JdbcTypeException(exception);
            } catch (IllegalAccessException exception) {
                throw new JdbcTypeException(exception);
            }
        }
    }

    private void releaseLobAfterAccess(Connection connection, Object lob) {
        if (isReleaseLobAfterAccess()) {
            initLobClasses(connection);
            Boolean temporary;
            try {
                Method isTemporary = lob.getClass().getMethod("isTemporary");
                temporary = (Boolean) isTemporary.invoke(lob);
                if (temporary) {
                    Method freeTemporary = lob.getClass().getMethod("freeTemporary");
                    freeTemporary.invoke(lob);
                } else {
                    Method isOpen = lob.getClass().getMethod("isOpen");
                    Boolean open = (Boolean) isOpen.invoke(lob);
                    if (open) {
                        Method close = lob.getClass().getMethod("close");
                        close.invoke(lob);
                    }
                }
            } catch (InvocationTargetException exception) {
                throw new JdbcTypeException(exception.getTargetException());
            } catch (NoSuchMethodException exception) {
                throw new JdbcTypeException(exception);
            } catch (IllegalAccessException exception) {
                throw new JdbcTypeException(exception);
            }
        }
    }

    private synchronized void initLobClasses(Connection connection) {
        if (!initLobClasses) {
            try {
                blobClass = connection.getClass().getClassLoader().loadClass(BLOB_CLASS_NAME);
                durationSessionConstants.put(blobClass, blobClass.getField(DURATION_SESSION_FIELD_NAME).getInt(null));
                modeReadWriteConstants.put(blobClass, blobClass.getField(MODE_READWRITE_FIELD_NAME).getInt(null));
                modeReadOnlyConstants.put(blobClass, blobClass.getField(MODE_READONLY_FIELD_NAME).getInt(null));

                clobClass = connection.getClass().getClassLoader().loadClass(CLOB_CLASS_NAME);
                durationSessionConstants.put(clobClass, clobClass.getField(DURATION_SESSION_FIELD_NAME).getInt(null));
                modeReadWriteConstants.put(clobClass, clobClass.getField(MODE_READWRITE_FIELD_NAME).getInt(null));
                modeReadOnlyConstants.put(clobClass, clobClass.getField(MODE_READONLY_FIELD_NAME).getInt(null));
            } catch (ClassNotFoundException exception) {
                throw new JdbcTypeException(exception);
            } catch (NoSuchFieldException exception) {
                throw new JdbcTypeException(exception);
            } catch (IllegalAccessException exception) {
                throw new JdbcTypeException(exception);
            }
            initLobClasses = true;
        }
    }

    public boolean isCache() {
        return cache;
    }

    public void setCache(boolean cache) {
        this.cache = cache;
    }

    public boolean isReleaseLobAfterAccess() {
        return releaseLobAfterAccess;
    }

    public void setReleaseLobAfterAccess(boolean releaseLobAfterAccess) {
        this.releaseLobAfterAccess = releaseLobAfterAccess;
    }
}
