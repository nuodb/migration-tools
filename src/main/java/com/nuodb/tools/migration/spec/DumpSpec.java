/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in connectionSpec and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of connectionSpec code must retain the above copyright
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
package com.nuodb.tools.migration.spec;

import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * @author Sergey Bushik
 */
public class DumpSpec extends TaskSpecBase {

    private ConnectionSpec connectionSpec;
    private Collection<SelectQuerySpec> selectQuerySpecs = Lists.newArrayList();
    private Collection<NativeQuerySpec> nativeQuerySpecs = Lists.newArrayList();
    private FormatSpec outputSpec;

    public ConnectionSpec getConnectionSpec() {
        return connectionSpec;
    }

    public void setConnectionSpec(ConnectionSpec connectionSpec) {
        this.connectionSpec = connectionSpec;
    }

    public Collection<SelectQuerySpec> getSelectQuerySpecs() {
        return selectQuerySpecs;
    }

    public void setSelectQuerySpecs(Collection<SelectQuerySpec> selectQuerySpecs) {
        this.selectQuerySpecs = selectQuerySpecs;
    }

    public Collection<NativeQuerySpec> getNativeQuerySpecs() {
        return nativeQuerySpecs;
    }

    public void setNativeQuerySpecs(Collection<NativeQuerySpec> nativeQuerySpecs) {
        this.nativeQuerySpecs = nativeQuerySpecs;
    }

    public FormatSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(FormatSpec outputSpec) {
        this.outputSpec = outputSpec;
    }
}
