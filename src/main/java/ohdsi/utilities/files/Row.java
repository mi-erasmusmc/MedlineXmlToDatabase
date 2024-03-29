/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ohdsi.utilities.files;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row {
    private List<String> cells;
    private Map<String, Integer> fieldName2ColumnIndex;

    public Row() {
        fieldName2ColumnIndex = new HashMap<String, Integer>();
        cells = new ArrayList<String>();
    }

    public String get(String fieldName) {
        int index;
        try {
            index = fieldName2ColumnIndex.get(fieldName);
        } catch (NullPointerException e) {
            throw new RuntimeException("Field \"" + fieldName + "\" not found");
        }
        if (cells.size() <= index)
            return null;
        else
            return cells.get(index);
    }

    public List<String> getFieldNames() {
        List<String> names = new ArrayList<String>(fieldName2ColumnIndex.size());
        for (int i = 0; i < fieldName2ColumnIndex.size(); i++)
            names.add(null);
        for (Map.Entry<String, Integer> entry : fieldName2ColumnIndex.entrySet())
            names.set(entry.getValue(), entry.getKey());
        return names;
    }

    public void add(String fieldName, String value) {
        fieldName2ColumnIndex.put(fieldName, cells.size());
        cells.add(value);
    }

    public void add(String fieldName, int value) {
        add(fieldName, Integer.toString(value));
    }

    public void add(String fieldName, boolean value) {
        add(fieldName, Boolean.toString(value));
    }

    public void add(String fieldName, double value) {
        add(fieldName, Double.toString(value));
    }

    public void add(String fieldName, long value) {
        add(fieldName, Long.toString(value));
    }

    public void set(String fieldName, String value) {
        cells.set(fieldName2ColumnIndex.get(fieldName), value);
    }

    public void set(String fieldName, int value) {
        set(fieldName, Integer.toString(value));
    }

    public void set(String fieldName, long value) {
        set(fieldName, Long.toString(value));
    }

    public void set(String fieldName, double value) {
        set(fieldName, Double.toString(value));
    }

    public String toString() {
        List<String> data = new ArrayList<String>(cells);
        for (String fieldName : fieldName2ColumnIndex.keySet()) {
            int index = fieldName2ColumnIndex.get(fieldName);
            if (data.size() > index)
                data.set(index, "[" + fieldName + ": " + data.get(index) + "]");
        }
        return String.join(",", data);
    }

    public void remove(String field) {
        Integer index = fieldName2ColumnIndex.remove(field);
        cells.remove(index);
        Map<String, Integer> tempMap = new HashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : fieldName2ColumnIndex.entrySet())
            if (entry.getValue() > index)
                tempMap.put(entry.getKey(), entry.getValue() - 1);
            else
                tempMap.put(entry.getKey(), entry.getValue());
    }

    public int size() {
        return cells.size();
    }

    public String get(int i) {
        return cells.get(i);
    }
}
