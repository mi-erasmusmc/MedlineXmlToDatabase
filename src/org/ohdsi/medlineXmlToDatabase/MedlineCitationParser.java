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
package org.ohdsi.medlineXmlToDatabase;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.ohdsi.databases.ConnectionWrapper;
import org.ohdsi.databases.ConnectionWrapper.FieldInfo;
import org.ohdsi.utilities.XmlTools;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.collections.OneToManySet;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * In this class, we do the actual work of reading the XML and inserting the data into the database
 * 
 * @author MSCHUEMI
 *
 */
public class MedlineCitationParser {
	
	private static String						MEDLINE_CITATION	= "MedlineCitation";
	private OneToManySet<String, String>		tables2Fields		= new OneToManySet<String, String>();
	private OneToManyList<String, FieldInfo>	tables2FieldInfos	= new OneToManyList<String, FieldInfo>();
	private String								pmid;
	private String								pmid_version;
	private ConnectionWrapper					connectionWrapper;
	
	public MedlineCitationParser(ConnectionWrapper connectionWrapper, String schema) {
		this.connectionWrapper = connectionWrapper;
		Set<String> tables = new HashSet<String>();
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
	
	public void parseAndInjectIntoDB(Node citation) {
		findPmidAndVersion(citation);

		connectionWrapper.setBatchMode(true);
		deleteAllForPmidAndVersion();
		Map<String, String> keys = new HashMap<>(4);
		keys.put("PMID", pmid);
		keys.put("PMID_Version", pmid_version);
		parseNode(citation, "", "MedlineCitation", new HashMap<>(32), true, keys);
		try {
			connectionWrapper.setBatchMode(false);
		} catch (Exception e) {
			System.err.println("Problem inserting in to DB for PMID " + pmid + ": " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * Record could be an update of a previous entry. Just in case, all previous data must be removed
	 * 
	 */
	private void deleteAllForPmidAndVersion() {
		for (String table : tables2Fields.keySet()) {
			String sql = "DELETE FROM " + Abbreviator.abbreviate(table) + " WHERE pmid = " + pmid + " AND pmid_version = " + pmid_version;
			connectionWrapper.execute(sql);
		}
		
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
					if (Abbreviator.abbreviate(field).toLowerCase().equals(fieldInfo.name.toLowerCase())) {
						name = field;
						break;
					}
				if (name != null) {
					String value = field2Value.get(name);
					try {
						Integer.parseInt(value);
					} catch (NumberFormatException e) {
						System.err.println("Error parsing integer value '" + value + "' for field " + fieldInfo.name + " in table " + table + ". Setting to null.");
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
			if (!fieldsInDb.contains(Abbreviator.abbreviate(field.toLowerCase()))) {
				System.err.println("Ignoring field " + field + " in table " + table
						+ " because field is not in DB (meaning it wasn't encountered in the XML files before now)");
				iterator.remove();
			}
		}
	}
	
	private void truncateFieldsToDbSize(String table, Map<String, String> field2Value) {
		for (FieldInfo fieldInfo : tables2FieldInfos.get(table.toLowerCase())) {
			if (fieldInfo.type == Types.VARCHAR || fieldInfo.type == Types.CLOB) {
				String name = null;
				for (String field : field2Value.keySet())
					if (Abbreviator.abbreviate(field).equalsIgnoreCase(fieldInfo.name.toLowerCase())) {
						name = field;
						break;
					}
				if (name != null) {
					String value = field2Value.get(name);
					if (value.length() > fieldInfo.length) {
						System.err.println("Truncating field " + fieldInfo.name + " in table " + table + " from " + value.length() + " to " + fieldInfo.length
								+ " characters for PMID " + pmid);
						value = value.substring(0, fieldInfo.length);
						field2Value.put(name, value);
					}
				}
			}
		}
	}
	
	private boolean findPmidAndVersion(Node node) {
		NodeList children = node.getChildNodes();
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);
			if (child.getNodeName().equals("PMID")) {
				pmid = child.getFirstChild().getNodeValue();
				pmid_version = child.getAttributes().getNamedItem("Version").getNodeValue();
				return true;
			}
			if (findPmidAndVersion(child))
				return true;
		}
		return false;
	}
	
	private void parseNode(Node node, String name, String tableName, HashMap<String, String> field2Value, boolean tableRoot, Map<String, String> keys) {
		// Add this value:
		if (node.getNodeValue() != null && node.getNodeValue().trim().length() != 0)
			field2Value.put(name.length() == 0 ? "Value" : name, node.getNodeValue());
		
		// Add attributes:
		NamedNodeMap attributes = node.getAttributes();
		if (attributes != null)
			for (int i = 0; i < attributes.getLength(); i++) {
				Node attribute = attributes.item(i);
				String attributeName = concatenate(name, attribute.getNodeName());
				field2Value.put(attributeName, attribute.getNodeValue());
			}
		
		if (XmlTools.isTextNode(node)) {
			field2Value.put(name.length() == 0 ? "Value" : name, node.getTextContent());
		} else {
			// Add children
			NodeList children = node.getChildNodes();
			int subCount = 1;
			for (int i = 0; i < children.getLength(); i++) {
				Node child = children.item(i);
				String childName = name;
				if (!child.getNodeName().equals("#text")) {
					childName = concatenate(childName, child.getNodeName());
				}
				String potentialNewTableName = concatenate(tableName, childName);
				if (tables2Fields.keySet().contains(potentialNewTableName.toLowerCase())) {// Its a sub table
					Map<String, String> newKeys = new HashMap<String, String>(keys);
					newKeys.put(potentialNewTableName + "_Order", Integer.toString(subCount++));
					parseNode(child, "", potentialNewTableName, new HashMap<String, String>(), true, newKeys);
				} else {
					parseNode(child, childName, tableName, field2Value, false, keys);
				}
			}
		}
		if (tableRoot) { // Bottom level completed: write values to database
			if (!tableName.equals(MEDLINE_CITATION)) {
				if (field2Value.containsKey("PMID")) {
					// A PMID field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
					field2Value.put("Other_PMID", field2Value.get("PMID"));
					field2Value.remove("PMID");
				}
				if (field2Value.containsKey("PMID_Version")) {
					// A PMID_Version field is encountered in a table that is not MEDLINE_CITATION. Need to rename to avoid collision with key
					field2Value.put("Other_PMID_Version", field2Value.get("PMID_Version"));
					field2Value.remove("PMID_Version");
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
	
	public void delete(Node node) {
		connectionWrapper.setBatchMode(true);
		NodeList children = node.getChildNodes();
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);
			if (child.getNodeName().equals("PMID")) {
				pmid = child.getFirstChild().getNodeValue();
				pmid_version = child.getAttributes().getNamedItem("Version").getNodeValue();
				deleteAllForPmidAndVersion();
			}
		}
		connectionWrapper.setBatchMode(false);
	}
}
