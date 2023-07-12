package com.nuodb.migrator.globalStore;

import java.util.*;

import com.google.common.collect.Lists;
import com.nuodb.migrator.backup.loader.LoadTable;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.HashMap;

public class GlobalStore {

    private static GlobalStore instance;
    private String state;

    HashMap<String, Boolean> generatedValues = new HashMap<>();

    public void put(String key, boolean value) {
        generatedValues.put(key, value);
    }

    public boolean get(String key) {
        return generatedValues.get(key);
    }

    private Collection<String> columns = Lists.newArrayList();

    private GlobalStore() {
        // Private constructor to enforce singleton pattern
    }

    public static synchronized GlobalStore getInstance() {
        if (instance == null) {
            instance = new GlobalStore();
        }
        return instance;
    }

    public synchronized String getState() {
        return state;
    }

    public synchronized void setState(String state) {
        this.state = state;

    }

    /**
     * Generates an ALTER statement based on the loaded table data. This method
     * creates an ALTER statement to modify columns in the table that have
     * associated sequences and a generator set to 'GENERATED ALWAYS AS
     * DEFAULT'.
     *
     * @param table
     *                  The LoadTable object representing the table with loaded
     *                  data.
     * @return The generated ALTER statement as a String if the column has
     *         'GENERATED ALWAYS AS DEFAULT' generator, or null if no
     *         alterations are needed or the generator is 'GENERATED BY DEFAULT
     *         AS IDENTITY'.
     */

    public String alterScript(LoadTable table) {

        StringBuilder str = new StringBuilder();
        for (Column col : table.getTable().getColumns()) {
            if (col.getSequence() != null) {
                str.append("ALTER TABLE ");
                str.append(table.getTable().getSchema().getName());
                str.append(".");
                str.append(table.getTable().getName());
                str.append(" MODIFY ");
                str.append(col.getName());
                str.append(' ');
                str.append(col.getJdbcType().getTypeName());
                str.append(' ');
                String key = col.getSequence().getName().toString();
                if (generatedValues == null)
                    return null;

                for (Map.Entry<String, Boolean> entry : generatedValues.entrySet()) {
                    String entryInHashMap = entry.getKey();
                    entryInHashMap.replaceAll("^\"|\"$", "");
                    entryInHashMap = entryInHashMap.replaceAll("^\"|\"$", "");
                    boolean valueInHashMap = entry.getValue();

                    if (key.equals(entryInHashMap)) {
                        if (valueInHashMap) {
                            str.append("GENERATED ALWAYS AS IDENTITY");
                            return str.toString();
                        }
                    }
                }

            }
        }
        return null;
    }
}
