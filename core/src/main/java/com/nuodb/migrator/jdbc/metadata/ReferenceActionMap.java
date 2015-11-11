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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.collect.Maps;

import java.util.Map;

import static com.nuodb.migrator.jdbc.metadata.ReferenceAction.*;
import static java.sql.DatabaseMetaData.*;

/**
 * @author Sergey Bushik
 */
public class ReferenceActionMap {

    private Map<Integer, ReferenceAction> referenceActionMap = Maps.newHashMap();

    private static ReferenceActionMap INSTANCE;

    public static ReferenceActionMap getInstance() {
        if (INSTANCE == null) {
            INSTANCE = createInstance();
        }
        return INSTANCE;
    }

    private static ReferenceActionMap createInstance() {
        ReferenceActionMap referenceActionMap = new ReferenceActionMap();
        referenceActionMap.add(importedKeyNoAction, NO_ACTION);
        referenceActionMap.add(importedKeyCascade, CASCADE);
        referenceActionMap.add(importedKeySetNull, SET_NULL);
        referenceActionMap.add(importedKeyRestrict, RESTRICT);
        referenceActionMap.add(importedKeySetDefault, SET_DEFAULT);
        return referenceActionMap;
    }

    public ReferenceAction get(int value) {
        return referenceActionMap.get(value);
    }

    public void add(int value, ReferenceAction action) {
        referenceActionMap.put(value, action);
    }
}
