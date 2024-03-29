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
package ohdsi.medlineXmlToDatabase;

import ohdsi.databases.ConnectionWrapper;
import ohdsi.utilities.StringUtilities;
import ohdsi.utilities.XmlTools;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In this class, we do the actual work of analyzing a single XML document.
 *
 * @author MSCHUEMI
 */
public class MedlineCitationAnalyser {

    private static final String MEDLINE_CITATION = "MedlineCitation";
    private static final String ORDER_POSTFIX = "_Order";
    private Map<String, Set<String>> table2Fields = new HashMap<>();
    private Map<String, VariableType> field2VariableType = new HashMap<>();

    public MedlineCitationAnalyser() {
        table2Fields.put(MEDLINE_CITATION, new HashSet<>());
    }

    public void analyse(Node citation) {
        analyseNode(citation, "", MEDLINE_CITATION);
    }

    /**
     * Call this method after analyzing all XML files.
     */
    public void finish() {
        cleanup();
        addKeys();
    }

    /**
     * Cleanup: remove sub table fields from higher level table
     */
    private void cleanup() {
        for (String table : table2Fields.keySet()) {
            Iterator<String> iterator = table2Fields.get(table).iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                for (String otherTable : table2Fields.keySet())
                    if (otherTable.length() != 0 && !table.startsWith(otherTable))
                        if (concatenate(table, fieldName).startsWith(otherTable)) {
                            iterator.remove();
                            break;
                        }
            }
        }
    }

    /**
     * Adds primary keys to all tables
     */
    private void addKeys() {
        for (String table : table2Fields.keySet()) {
            String parent = table;
            while (parent.contains("_")) {
                parent = parent.substring(0, parent.lastIndexOf('_'));
                if (!parent.equals(MEDLINE_CITATION) && table2Fields.containsKey(parent)) {
                    table2Fields.get(table).add(parent + ORDER_POSTFIX);
                    field2VariableType.put(concatenate(table, parent + ORDER_POSTFIX), new VariableType(true, 3));
                }
            }
            if (!table.equals(MEDLINE_CITATION)) {
                Set<String> fields = table2Fields.get(table);
                fields.add(table + ORDER_POSTFIX);
                field2VariableType.put(concatenate(table, table + ORDER_POSTFIX), new VariableType(true, 3));
                if (fields.contains("PMID")) {
                    // A PMID field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
                    fields.add("Other_PMID");
                    fields.remove("PMID");
                    VariableType variableType = field2VariableType.get(concatenate(table, "PMID"));
                    field2VariableType.put(concatenate(table, "Other_PMID"), variableType);
                    field2VariableType.remove(concatenate(table, "PMID"));
                }
                if (fields.contains("PMID_Version")) {
                    // A PMID_Version field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
                    fields.add("Other_PMID_Version");
                    fields.remove("PMID_Version");
                    VariableType variableType = field2VariableType.get(concatenate(table, "PMID_Version"));
                    field2VariableType.put(concatenate(table, "Other_PMID_Version"), variableType);
                    field2VariableType.remove(concatenate(table, "OtPMID_Versionher_PMID"));
                }
            }
            table2Fields.get(table).add("PMID");
            field2VariableType.put(concatenate(table, "PMID"), new VariableType(true, 8));
            table2Fields.get(table).add("PMID_Version");
            field2VariableType.put(concatenate(table, "PMID_Version"), new VariableType(true, 1));
        }
    }

    public void printStructure() {
        List<String> sortedTables = new ArrayList<String>(table2Fields.keySet());
        Collections.sort(sortedTables);
        for (String table : sortedTables) {
            List<String> sortedFields = new ArrayList<String>(table2Fields.get(table));
            Collections.sort(sortedFields);
            System.out.println("\n" + table);
            for (String field : sortedFields)
                System.out.println("- " + field + "\t" + field2VariableType.get(concatenate(table, field)));
        }
    }

    public void createTables(ConnectionWrapper connectionWrapper) {
        List<String> sortedTables = new ArrayList<String>(table2Fields.keySet());
        Collections.sort(sortedTables);
        for (String table : sortedTables) {

            List<String> sortedFields = new ArrayList<String>(table2Fields.get(table));
            Collections.sort(sortedFields);
            List<VariableType> types = new ArrayList<VariableType>(sortedFields.size());
            for (String field : sortedFields)
                types.add(field2VariableType.get(concatenate(table, field)));
            int index = sortedFields.indexOf("");
            if (index != -1)
                sortedFields.set(index, "Value");
            List<String> primaryKey = new ArrayList<String>();
            primaryKey.add("PMID");
            primaryKey.add("PMID_Version");
            for (String field : sortedFields)
                if (field.endsWith(ORDER_POSTFIX))
                    primaryKey.add(field);

            connectionWrapper.createTableUsingVariableTypes(table, sortedFields, types, primaryKey);
        }
    }

    private boolean hasIllegalCharacter(String name) {
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (!Character.isLetterOrDigit(ch) && ch != '_') {
                return true;
            }
        }
        return false;
    }

    private void analyseNode(Node node, String name, String tableName) {
        // Add attributes:
        NamedNodeMap attributes = node.getAttributes();
        if (attributes != null)
            for (int i = 0; i < attributes.getLength(); i++) {
                Node attribute = attributes.item(i);
                String attributeName = concatenate(name, attribute.getNodeName());
                if (hasIllegalCharacter(attributeName))
                    System.err.println("Illegal character found in name '" + attributeName + "' in element '" + tableName + "'. Skipping field. ");
                else {
                    table2Fields.get(tableName).add(attributeName);
                    updateVariableType(concatenate(tableName, attributeName), attribute.getNodeValue());
                }
            }
        if (XmlTools.isTextNode(node)) {
            if (hasIllegalCharacter(name))
                System.err.println("Illegal character found in name '" + name + "' in element '" + tableName + "'. Skipping field. ");
            else {
                table2Fields.get(tableName).add(name);
                String value = node.getTextContent();
                updateVariableType(concatenate(tableName, name), value);
            }
        } else {
            // Add children
            NodeList children = node.getChildNodes();
            if (children.getLength() > 0) {
                Set<String> seenChildren = new HashSet<String>();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    String childName = name;
                    String tempTableName = tableName;
                    if (!child.getNodeName().equals("#text")) {
                        childName = concatenate(childName, child.getNodeName());
                        String potentialNewTableName = concatenate(tableName, childName);
                        if (!seenChildren.add(childName)) { // Multiple instances per citation: must make it a sub table
                            if (!table2Fields.containsKey(potentialNewTableName)) {
                                Set<String> fields = new HashSet<String>();
                                table2Fields.put(potentialNewTableName, fields);
                            }
                            tempTableName = potentialNewTableName;
                            childName = "";
                        } else if (table2Fields.containsKey(potentialNewTableName)) {// Already know its a sub table
                            tempTableName = potentialNewTableName;
                            childName = "";
                        }
                    }
                    if (hasIllegalCharacter(childName)) {
                        System.err.println("Illegal character found in name '" + childName + "' in element '" + tempTableName + "'. Skipping field. ");
                        continue;
                    }

                    if (child.getNodeValue() != null && child.getNodeValue().trim().length() != 0) {
                        table2Fields.get(tempTableName).add(childName);
                        updateVariableType(concatenate(tempTableName, childName), child.getNodeValue());
                    }
                    analyseNode(child, childName, tempTableName);
                }
            }
        }
    }

    private void updateVariableType(String name, String value) {
        VariableType type = field2VariableType.get(name);
        if (type == null) {
            type = new VariableType();
            field2VariableType.put(name, type);
        }

        if (type.isNumeric && !StringUtilities.isInteger(value))
            type.isNumeric = false;

        if (value.length() > type.maxLength)
            type.maxLength = value.length();
    }

    private String concatenate(String pre, String post) {
        if (pre.length() != 0)
            return pre + "_" + post;
        else
            return post;
    }

    public class VariableType {
        public boolean isNumeric = true;
        public int maxLength = 0;

        public VariableType(boolean isNumeric, int maxLength) {
            this.isNumeric = isNumeric;
            this.maxLength = maxLength;
        }

        public VariableType() {
        }

        public String toString() {
            if (isNumeric)
                return "INT";
            else
                return "VARCHAR";
        }

    }
}
