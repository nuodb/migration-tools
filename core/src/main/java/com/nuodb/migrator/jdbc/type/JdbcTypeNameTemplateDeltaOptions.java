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
package com.nuodb.migrator.jdbc.type;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("ConstantConditions")
public class JdbcTypeNameTemplateDeltaOptions extends JdbcTypeNameTemplate {

    private JdbcTypeOptions deltaOptions;

    public JdbcTypeNameTemplateDeltaOptions(String template, JdbcTypeOptions deltaOptions) {
        super(template);
        this.deltaOptions = deltaOptions;
    }

    public JdbcTypeNameTemplateDeltaOptions(JdbcType jdbcType, String template, JdbcTypeOptions deltaOptions) {
        super(jdbcType, template);
        this.deltaOptions = deltaOptions;
    }

    @Override
    protected String expandPrecision(String template, Integer precision) {
        Integer precisionDelta = getDeltaPrecision();
        return super.expandPrecision(template,
                precisionDelta != null && precision != null ? precisionDelta + precision : precision);
    }

    @Override
    protected String expandSize(String template, Long size) {
        Long sizeDelta = getDeltaSize();
        return super.expandSize(template, sizeDelta != null && size != null ? sizeDelta + size : size);
    }

    @Override
    protected String expandScale(String template, Integer scale) {
        Integer scaleDelta = getDeltaScale();
        return super.expandScale(template, scaleDelta != null && scale != null ? scaleDelta + scale : scale);
    }

    protected Long getDeltaSize() {
        return deltaOptions.getSize();
    }

    protected Integer getDeltaPrecision() {
        return deltaOptions.getPrecision();
    }

    protected Integer getDeltaScale() {
        return deltaOptions.getScale();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        JdbcTypeNameTemplateDeltaOptions jdbcTypeNameDeltaOptions = (JdbcTypeNameTemplateDeltaOptions) o;

        if (deltaOptions != null ? !deltaOptions.equals(jdbcTypeNameDeltaOptions.deltaOptions)
                : jdbcTypeNameDeltaOptions.deltaOptions != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (deltaOptions != null ? deltaOptions.hashCode() : 0);
        return result;
    }
}
