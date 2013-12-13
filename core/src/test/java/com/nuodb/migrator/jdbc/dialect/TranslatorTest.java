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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;

import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.MYSQL;
import static java.sql.Types.DATE;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class TranslatorTest {


    @DataProvider(name = "translate")
    public Object[][] createTranslateData() {

        Session session = mock(Session.class);
        when(session.getDialect()).thenReturn(new MySQLDialect(MYSQL));

        ConnectionProxy connectionProxy = mock(ConnectionProxy.class, withSettings().extraInterfaces(Connection.class));
        when(session.getConnection()).thenReturn((Connection) connectionProxy);

        // tests creation of implicit defaults for date, time & timestamp types
        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setUrl("jdbc:mysql://localhost/test?zeroDateTimeBehavior=round");
        when(connectionProxy.getConnectionSpec()).thenReturn(connectionSpec);

        Column date = new Column("f1");
        date.setTypeCode(DATE);

        Column time = new Column("f1");
        time.setTypeCode(TIME);

        Column timestamp = new Column("f1");
        timestamp.setTypeCode(TIMESTAMP);

        TranslationManager translationManager = new TranslationManager();
        translationManager.addTranslator(new MySQLZeroDateTimeTranslator());
        MySQLImplicitDefaultsTranslator translator = new MySQLImplicitDefaultsTranslator();
        translator.setUseExplicitDefaults(true);
        return new Object[][]{
                {
                        translator,
                        new ColumnScript(date, session),
                        new SimpleTranslationContext(MYSQL, translationManager),
                        "0001-01-01"
                },
                {
                        translator,
                        new ColumnScript(time, session),
                        new SimpleTranslationContext(MYSQL, translationManager),
                        "00:00:00"
                },
                {
                        translator,
                        new ColumnScript(timestamp, session),
                        new SimpleTranslationContext(MYSQL, translationManager),
                        "0001-01-01 00:00:00"
                }
        };
    }

    @Test(dataProvider = "translate")
    public void testTranslate(Translator translator, Script script, TranslationContext context, String translation) {
        assertTrue(translator.supports(script, context));
        assertEquals(translator.translate(script, context).getScript(), translation);
    }
}
