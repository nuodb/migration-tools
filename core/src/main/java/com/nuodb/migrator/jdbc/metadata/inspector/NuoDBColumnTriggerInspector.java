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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.utils.RegexUtils;
import com.nuodb.migrator.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN_TRIGGER;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;

/**
 * @author Mukund
 */
public class NuoDBColumnTriggerInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String TRIGGERTEXT_REG_EXPRESSION = "NEW.`(.*?)`(.*)";

    public NuoDBColumnTriggerInspector() {
        super(COLUMN_TRIGGER, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery triggers = new SelectQuery();
        Collection<String> parameters = newArrayList();
        triggers.columns("SCHEMA", "TABLENAME", "TRIGGERNAME", "TRIGGER_TYPE", "TYPE_MASK", "POSITION", "ACTIVE",
                "TRIGGER_TEXT");
        triggers.from("SYSTEM.TRIGGERS");

        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            triggers.where("TABLENAME=?");
            parameters.add(table);
        }
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            triggers.where("SCHEMA=?");
            parameters.add(schema);
        }
        return new ParameterizedQuery(triggers, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet triggers) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        String TRIGGEREVENT = null;
        String TRIGGERTIME = null;
        boolean active = false;
        while (triggers.next()) {
            if (triggers.getInt("TRIGGER_TYPE") == 1) {
                switch (triggers.getInt("TYPE_MASK")) {
                case 1:
                    TRIGGERTIME = "BEFORE";
                    TRIGGEREVENT = "INSERT";
                    break;
                case 2:
                    TRIGGERTIME = "AFTER";
                    TRIGGEREVENT = "INSERT";
                    break;
                case 4:
                    TRIGGERTIME = "BEFORE";
                    TRIGGEREVENT = "UPDATE";
                    break;
                case 8:
                    TRIGGERTIME = "AFTER";
                    TRIGGEREVENT = "UPDATE";
                    break;
                case 16:
                    TRIGGERTIME = "BEFORE";
                    TRIGGEREVENT = "DELETE";
                    break;
                case 32:
                    TRIGGERTIME = "AFTER";
                    TRIGGEREVENT = "DELETE";
                    break;
                case 64:
                    TRIGGERTIME = "BEFORE";
                    TRIGGEREVENT = "COMMIT";
                    break;
                case 128:
                    TRIGGERTIME = "AFTER";
                    TRIGGEREVENT = "COMMIT";
                    break;
                }
                switch (triggers.getInt("ACTIVE")) {
                case 1:
                    active = true;
                    break;
                case 0:
                    active = false;
                    break;
                }

                Table table = inspectionResults.getObject(MetaDataType.TABLE, triggers.getString("TABLENAME"));
                ColumnTrigger columnTrigger = new ColumnTrigger();
                String columnName = RegexUtils.getRegexGroup(TRIGGERTEXT_REG_EXPRESSION,
                        triggers.getString("TRIGGER_TEXT"), 1);
                if (!StringUtils.isEmpty(columnName)) {
                    columnTrigger.setColumn(table.getColumn(columnName));
                }
                columnTrigger.setTriggerEvent(TriggerEvent.valueOf(TRIGGEREVENT));
                columnTrigger.setTriggerTime(TriggerTime.valueOf(TRIGGERTIME));
                columnTrigger.setTriggerBody(getTriggerBody(triggers.getString("TRIGGER_TEXT")));
                table.addTrigger(columnTrigger);
                inspectionResults.addObject(columnTrigger);
            }
        }
    }

    private String getTriggerBody(String tString) {
        String triggerBody = null;
        if (!tString.isEmpty()) {
            String triggerText[] = tString.split(";");
            triggerBody = triggerText[0] + ";";
        }
        return triggerBody;
    }
}
