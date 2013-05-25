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
package com.nuodb.migrator.jdbc.type;

/**
 * Changes target types specifiers adjusted by specified delta. For instance MySQL BIGINT(20) UNSIGNED will be rendered
 * as NUMERIC(21) by JdbcTypeNameChangeSpecifiersBuilder("NUMERIC({N})", 1));
 *
 * @author Sergey Bushik
 */
public class JdbcTypeNameChangeSpecifiersBuilder extends JdbcTypeNameTemplateBuilder {

    private Integer sizeDelta;
    private Integer precisionDelta;
    private Integer scaleDelta;

    public JdbcTypeNameChangeSpecifiersBuilder(String template) {
        super(template);
    }

    public JdbcTypeNameChangeSpecifiersBuilder(String template, Integer sizeDelta) {
        super(template);
        this.sizeDelta = sizeDelta;
    }

    @Override
    protected String expandPrecision(String template, Integer precision) {
        Integer precisionDelta = getPrecisionDelta();
        return super.expandPrecision(template, precisionDelta != null && precision != null ?
                precisionDelta + precision : precision);
    }

    @Override
    protected String expandSize(String template, Integer size) {
        Integer sizeDelta = getSizeDelta();
        return super.expandSize(template, sizeDelta != null && size != null ? sizeDelta + size : size);
    }

    @Override
    protected String expandScale(String template, Integer scale) {
        Integer scaleDelta = getScaleDelta();
        return super.expandScale(template, scaleDelta != null && scale != null ? scaleDelta + scale : scale);
    }

    public Integer getSizeDelta() {
        return sizeDelta;
    }

    public void setSizeDelta(Integer sizeDelta) {
        this.sizeDelta = sizeDelta;
    }

    public Integer getPrecisionDelta() {
        return precisionDelta;
    }

    public void setPrecisionDelta(Integer precisionDelta) {
        this.precisionDelta = precisionDelta;
    }

    public Integer getScaleDelta() {
        return scaleDelta;
    }

    public void setScaleDelta(Integer scaleDelta) {
        this.scaleDelta = scaleDelta;
    }
}
