package ohdsi.meshXmlToDatabase;

import ohdsi.databases.InsertableDbTable;
import ohdsi.utilities.StringUtilities;
import ohdsi.utilities.files.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class SupplementaryMeshParser extends DefaultHandler {

    private static final Logger log = LogManager.getLogger(SupplementaryMeshParser.class.getName());


    private final InsertableDbTable outTerms;
    private final InsertableDbTable outRelationship;
    private Row row;
    private StringBuilder recordName;
    private Trace trace = new Trace();
    private String ui;

    public SupplementaryMeshParser(InsertableDbTable outTerms, InsertableDbTable outRelationship) {
        super();
        this.outRelationship = outRelationship;
        this.outTerms = outTerms;
    }

    public static void parse(String fileName, InsertableDbTable outTerms, InsertableDbTable outRelationship) {
        log.info("Parsing supplement file");
        try (FileInputStream fileInputStream = new FileInputStream(fileName);
             GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
            SupplementaryMeshParser mainMeshParser = new SupplementaryMeshParser(outTerms, outRelationship);
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
        if (name.equalsIgnoreCase("SupplementalRecord")) {
            row = new Row();
            recordName = new StringBuilder();
        }
    }

    @Override
    public void characters(char ch[], int start, int length) {
        String traceString = trace.toString();
        if (traceString.equalsIgnoreCase("SupplementalRecordSet.SupplementalRecord.SupplementalRecordUI")) {
            ui = new String(ch, start, length);
            row.add("ui", ui);
        } else if (traceString.equalsIgnoreCase("SupplementalRecordSet.SupplementalRecord.SupplementalRecordName.String")) {
            if (recordName.length() != 0)
                recordName.append(' ');
            recordName.append(new String(ch, start, length));
        } else if (traceString
                .equalsIgnoreCase("SupplementalRecordSet.SupplementalRecord.HeadingMappedToList.HeadingMappedTo.DescriptorReferredTo.DescriptorUI")) {
            Row rowPa = new Row();
            rowPa.add("ui_1", ui);
            rowPa.add("ui_2", new String(ch, start, length).replace("*", ""));
            rowPa.add("relationship_id", "Maps to");
            outRelationship.write(rowPa);
        } else if (traceString.equalsIgnoreCase(
                "SupplementalRecordSet.SupplementalRecord.PharmacologicalActionList.PharmacologicalAction.DescriptorReferredTo.DescriptorUI")) {
            Row rowPa = new Row();
            rowPa.add("ui_1", ui);
            rowPa.add("ui_2", new String(ch, start, length));
            rowPa.add("relationship_id", "Pharmacological action");
            outRelationship.write(rowPa);
        }
    }

    public void endElement(String uri, String localName, String name) {
        trace.pop();
        if (name.equalsIgnoreCase("SupplementalRecord")) {
            row.add("name", recordName.toString());
            row.add("supplement", "1");
            outTerms.write(row);
        }
    }

    private static class Trace {
        private List<String> tags = new ArrayList<>();

        public void push(String tag) {
            tags.add(tag);
        }

        public void pop() {
            tags.remove(tags.size() - 1);
        }

        public String toString() {
            return String.join(".", tags);
        }
    }

}
