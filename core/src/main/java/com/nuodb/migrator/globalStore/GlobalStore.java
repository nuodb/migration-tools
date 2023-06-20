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

    private Table table;

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

    public void setState(String state) {
        this.state = state;

    }

    // helper function to create an alter statement to execute after loading the
    // data, takes the value from hashmap and compares it.
    public String alterScript(LoadTable table) {
        try {
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
                    for (Map.Entry<String, Boolean> entry : generatedValues.entrySet()) {
                        String entryInHashMap = entry.getKey();
                        entryInHashMap.replaceAll("^\"|\"$", "");
                        entryInHashMap = entryInHashMap.replaceAll("^\"|\"$", "");
                        boolean valueInHashMap = entry.getValue();
                        if (generatedValues != null) {
                            if (key.equals(entryInHashMap)) {
                                if (valueInHashMap) {
                                    str.append("GENERATED ALWAYS AS IDENTITY");
                                    return str.toString();
                                } else {
                                    return null;
                                }
                            } else {
                                continue;
                            }
                        } else
                            return null;
                    }
                }
            }
            return null;
        } catch (Exception e) {
            System.out.println("Caught Exception in GlobalStore");
        }

        return null;
    }
}
