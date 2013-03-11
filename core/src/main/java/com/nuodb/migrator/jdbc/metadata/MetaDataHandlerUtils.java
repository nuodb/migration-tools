/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.metadata;

import java.util.Collection;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class MetaDataHandlerUtils {

    public static <T extends MetaDataHandler> T findMetaDataHandler(Collection<? extends T> objectHandlers,
                                                                    MetaData object) {
        return findMetaDataHandler(objectHandlers, object.getObjectType(), true);
    }

    public static <T extends MetaDataHandler> T findMetaDataHandler(Collection<? extends T> objectHandlers,
                                                                    MetaData object, boolean required) {
        return findMetaDataHandler(objectHandlers, object.getObjectType(), required);
    }

    public static <T extends MetaDataHandler> T findMetaDataHandler(Collection<? extends T> objectHandlers,
                                                                    MetaDataType objectType) {
        return findMetaDataHandler(objectHandlers, objectType, true);
    }

    public static <T extends MetaDataHandler> T findMetaDataHandler(Collection<? extends T> objectHandlers,
                                                                    MetaDataType objectType, boolean required) {
        for (T objectHandler : objectHandlers) {
            if (objectHandler.supports(objectType)) {
                return objectHandler;
            }
        }
        if (required) {
            throw new MetaDataException(format("Required %s handler not found", objectType));
        } else {
            return null;
        }
    }
}
