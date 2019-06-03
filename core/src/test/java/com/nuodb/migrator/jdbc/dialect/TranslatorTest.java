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

import com.nuodb.migrator.jdbc.session.Session;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.dialect.TranslatorUtils.createScript;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static java.sql.Types.*;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class TranslatorTest {

    @DataProvider(name = "translate")
    public Object[][] createTranslateData() throws SQLException {
        Session session = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost/test?zeroDateTimeBehavior=round");
        TranslationManager translationManager = new TranslationManager();
        translationManager.addTranslator(new MySQLZeroDateTimeTranslator());
        translationManager.getTranslationConfig().setUseExplicitDefaults(true);
        Dialect dialect = createService(DialectResolver.class).resolve(MYSQL);
        return new Object[][] {
                { new SimpleTranslationContext(dialect, session, translationManager),
                        createScript("0000-00-00", DATE, "DATE"), "0001-01-01" },
                { new SimpleTranslationContext(dialect, session, translationManager),
                        createScript("00:00:00", TIME, "TIME"), "00:00:00" },
                { new SimpleTranslationContext(dialect, session, translationManager),
                        createScript("0000-00-00 00:00:00", TIMESTAMP, "TIMESTAMP"), "0001-01-01 00:00:00" } };
    }

    @Test(dataProvider = "translate")
    public void testTranslate(TranslationContext context, Script script, String translation) {
        assertEquals(context.translate(script).getScript(), translation);
    }
}
