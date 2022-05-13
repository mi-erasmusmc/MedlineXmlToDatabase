package ohdsi.meshXmlToDatabase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import ohdsi.databases.InsertableDbTable;
import ohdsi.utilities.StringUtilities;
import ohdsi.utilities.files.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class MainMeshParser extends DefaultHandler {

	private static final Logger log = LogManager.getLogger(MainMeshParser.class.getName());

	private final InsertableDbTable	outTerms;
	private final InsertableDbTable	outRelationship;
	private final Map<String, String>	treeNumberToUi;
	private Row row;
	private Trace trace = new Trace();
	private String ui;
	
	public MainMeshParser(InsertableDbTable outTerms, InsertableDbTable outRelationship, Map<String, String>	treeNumberToUi) {
		super();
		this.outRelationship = outRelationship;
		this.outTerms = outTerms;
		this.treeNumberToUi = treeNumberToUi;
	}

	public static void parse(String fileName, InsertableDbTable outTerms, InsertableDbTable outRelationship, Map<String, String>	treeNumberToUi) {
		log.info("Parsing main file");
		try (FileInputStream fileInputStream = new FileInputStream(fileName);
			GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
			MainMeshParser mainMeshParser = new MainMeshParser(outTerms, outRelationship, treeNumberToUi);
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(gzipInputStream, mainMeshParser);
		} catch (SAXException | ParserConfigurationException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startElement(String uri, String localName, String name, Attributes a) {
		trace.push(name);
		if (name.equalsIgnoreCase("DescriptorRecord")) 
			row = new Row();
	}


	@Override
	public void characters(char[] ch, int start, int length) {
		String traceString = trace.toString();
		if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.DescriptorUI")) {
			ui = new String(ch, start, length);
			row.add("ui", ui);
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.DescriptorName.String")) {
			row.add("name", new String(ch, start, length));
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.TreeNumberList.TreeNumber")) {
			treeNumberToUi.put(new String(ch, start, length), ui);
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.PharmacologicalActionList.PharmacologicalAction.DescriptorReferredTo.DescriptorUI")) {
			Row rowPa = new Row();
			rowPa.add("ui_1", ui);
			rowPa.add("ui_2", new String(ch, start, length));
			rowPa.add("relationship_id", "Pharmacological action");
			outRelationship.write(rowPa);
		}
	}

	@Override
	public void endElement(String uri, String localName, String name) {
		trace.pop();
		if (name.equalsIgnoreCase("DescriptorRecord")) {
			row.add("supplement", "0");
			outTerms.write(row);
		}
	}
	
	private static class Trace {
		private List<String> tags = new ArrayList<>();
		
		public void push(String tag) {
			tags.add(tag);
		}
		
		public void pop() {
			tags.remove(tags.size()-1);
		}
		
		public String toString() {
			return StringUtilities.join(tags, ".");
		}
	}

}
