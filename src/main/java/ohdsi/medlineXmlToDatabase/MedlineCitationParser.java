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
import ohdsi.databases.ConnectionWrapper.FieldInfo;
import ohdsi.utilities.XmlTools;
import ohdsi.utilities.collections.OneToManyList;
import ohdsi.utilities.collections.OneToManySet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In this class, we do the actual work of reading the XML and inserting the data into the database
 *
 * @author MSCHUEMI
 */
public class MedlineCitationParser {

    private static final Logger log = LogManager.getLogger(MedlineCitationParser.class.getName());

    private static final String MEDLINE_CITATION = "MedlineCitation";
    private static final String VERSION = "Version";
    private static final String PMID_CONSTANT = "PMID";
    public static final String PMID_VERSION_CONSTANT = "PMID_Version";
    private final OneToManySet<String, String> tables2Fields = new OneToManySet<>();
    private final OneToManyList<String, FieldInfo> tables2FieldInfos = new OneToManyList<>();
    private String pmid;
    private String pmidVersion;
    private final ConnectionWrapper connectionWrapper;

    public MedlineCitationParser(ConnectionWrapper connectionWrapper, String schema) {
        this.connectionWrapper = connectionWrapper;
        Set<String> tables = new HashSet<>();
        for (String table : connectionWrapper.getTableNames(schema)) {
            table = Abbreviator.unAbbreviate(table);
            if (table.toLowerCase().startsWith("medlinecitation"))
                tables.add(table);
        }
        for (String table : tables)
            for (FieldInfo fieldInfo : connectionWrapper.getFieldInfo(Abbreviator.abbreviate(table))) {
                tables2Fields.put(table, fieldInfo.name);
                tables2FieldInfos.put(table, fieldInfo);
            }
    }

    public void parseAndInjectIntoDB(Node citation, boolean updateFiles) {
        findPmidAndVersion(citation);
        if (connectionWrapper.existsForPMIDAndVersion(pmid, pmidVersion)) {
            if (updateFiles) {
                deleteAllForPMIDAndVersion();
            } else {
                return;
            }
        }
        Map<String, String> keys = Map.of(PMID_CONSTANT, pmid, PMID_VERSION_CONSTANT, pmidVersion);
        parseNode(citation, "", MEDLINE_CITATION, new HashMap<>(44), true, keys);
    }

    /**
     * Record could be an update of a previous entry. Just in case, all previous data must be removed
     */
    private void deleteAllForPMIDAndVersion() {
        connectionWrapper.deleteAllForPMIDAndVersion(tables2Fields.keySet(), pmid, pmidVersion);
    }

    private void insertIntoDB(String table, Map<String, String> field2Value) {
        removeFieldsNotInDb(table, field2Value);
        truncateFieldsToDbSize(table, field2Value);
        dropInvalidValues(table, field2Value);
        connectionWrapper.insertIntoTable(table, field2Value);
    }

    private void dropInvalidValues(String table, Map<String, String> field2Value) {
        for (FieldInfo fieldInfo : tables2FieldInfos.get(table.toLowerCase())) {
            if (fieldInfo.type == Types.INTEGER || fieldInfo.type == Types.BIGINT) {
                String name = null;
                for (String field : field2Value.keySet())
                    if (Abbreviator.abbreviate(field).equalsIgnoreCase(fieldInfo.name)) {
                        name = field;
                        break;
                    }
                if (name != null) {
                    String value = field2Value.get(name);
                    try {
                        Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        log.warn("Error parsing integer value '{}' for field {} in table {}. Setting to null", value, fieldInfo.name, table);
                        field2Value.remove(name);
                    }
                }
            }
        }
    }

    private void removeFieldsNotInDb(String table, Map<String, String> field2Value) {
        Set<String> fieldsInDb = tables2Fields.get(table.toLowerCase());
        Iterator<Map.Entry<String, String>> iterator = field2Value.entrySet().iterator();
        while (iterator.hasNext()) {
            String field = iterator.next().getKey();
            if (!fieldsInDb.contains(Abbreviator.abbreviate(field))) {
                log.warn("Ignoring '{}' in '{}', field was not encountered in the XML files when creating schema", field, table);
                iterator.remove();
            }
        }
    }

    private void truncateFieldsToDbSize(String table, Map<String, String> field2Value) {
        for (FieldInfo fieldInfo : tables2FieldInfos.get(table.toLowerCase())) {
            if (fieldInfo.type == Types.VARCHAR || fieldInfo.type == Types.CLOB) {
                String name = null;
                for (String field : field2Value.keySet())
                    if (Abbreviator.abbreviate(field).equalsIgnoreCase(fieldInfo.name)) {
                        name = field;
                        break;
                    }
                if (name != null) {
                    String value = field2Value.get(name);
                    if (value.length() > fieldInfo.length) {
                        log.warn("Truncating field {} in table {} from {} to {} characters for PMID {}", fieldInfo.name, table, value.length(), fieldInfo.length, pmid);
                        value = value.substring(0, fieldInfo.length);
                        field2Value.put(name, value);
                    }
                }
            }
        }
    }

    private void findPmidAndVersion(Node node) {
        org.dom4j.Element element = (Element) node.selectSingleNode("./PMID");
        pmid = element.getStringValue();
        pmidVersion = element.attributeValue(VERSION);
    }

    private void parseNode(Node node, String name, String tableName, HashMap<String, String> field2Value, boolean tableRoot, Map<String, String> keys) {
        // Add this value:
        if (((Element) node).getTextTrim().length() != 0) {
            field2Value.put(name.length() == 0 ? "Value" : name, node.getStringValue());
        }

        // Add attributes:
        ((Element) node).attributes().forEach(attribute -> {
            String attributeName = concatenate(name, attribute.getName());
            field2Value.put(attributeName, attribute.getValue());
        });


        if (XmlTools.isTextNode(node)) {
            field2Value.put(name.length() == 0 ? "Value" : name, node.getText());
        } else {
            // Add children
            List<org.dom4j.Node> children = node.selectNodes("./*");
            AtomicInteger subCount = new AtomicInteger(1);
            children.forEach(child -> {
                String childName = name;
                if (!child.getName().equals("#text")) {
                    childName = concatenate(childName, child.getName());
                }
                String potentialNewTableName = concatenate(tableName, childName);
                if (tables2Fields.keySet().contains(potentialNewTableName.toLowerCase())) {// Its a sub table
                    Map<String, String> newKeys = new HashMap<>(keys);
                    newKeys.put(potentialNewTableName + "_Order", Integer.toString(subCount.getAndIncrement()));
                    parseNode(child, "", potentialNewTableName, new HashMap<>(), true, newKeys);
                } else {
                    parseNode(child, childName, tableName, field2Value, false, keys);
                }
            });
        }
        if (tableRoot) { // Bottom level completed: write values to database
            if (!tableName.equals(MEDLINE_CITATION)) {
                if (field2Value.containsKey(PMID_CONSTANT)) {
                    // A PMID field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
                    field2Value.put("Other_PMID", field2Value.get(PMID_CONSTANT));
                    field2Value.remove(PMID_CONSTANT);
                }
                if (field2Value.containsKey(PMID_VERSION_CONSTANT)) {
                    // A PMID_Version field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
                    field2Value.put("Other_PMID_Version", field2Value.get(PMID_VERSION_CONSTANT));
                    field2Value.remove(PMID_VERSION_CONSTANT);
                }
            }
            field2Value.putAll(keys);
            insertIntoDB(tableName, field2Value);
        }
    }

    private String concatenate(String pre, String post) {
        if (pre.length() != 0)
            return pre + "_" + post;
        else
            return post;
    }

    public void delete(org.dom4j.Node node) {
        pmid = node.getStringValue();
        pmidVersion = ((Element) node).attributeValue(VERSION);
        deleteAllForPMIDAndVersion();
    }
}
