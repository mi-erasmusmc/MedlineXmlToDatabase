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
import ohdsi.databases.DbType;
import ohdsi.utilities.files.IniFile;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

/**
 * This class analyzes the XML files and creates the appropriate database structure
 *
 * @author Schuemie
 */
public class MedlineAnalyserMain {

    /**
     * Specifies the maximum number of files to randomly sample.
     */
    public static final int MAX_FILES_TO_ANALYSE = 1000;

    private MedlineCitationAnalyser medlineCitationAnalyser;

    public static void main(String[] args) {
        IniFile iniFile = new IniFile(args[0]);

        MedlineAnalyserMain main = new MedlineAnalyserMain();
        main.analyseFolder(iniFile.get("XML_FOLDER"));
        main.createDatabase(iniFile.get("SERVER"), iniFile.get("SCHEMA"), iniFile.get("USER"), iniFile.get("PASSWORD"),
                iniFile.get("DATA_SOURCE_TYPE"), iniFile.get("CREATE_SCHEMA"));
    }

    private void analyseFolder(String folderName) {
        medlineCitationAnalyser = new MedlineCitationAnalyser();

        List<File> files = Arrays.asList(Objects.requireNonNull(new File(folderName).listFiles()));
        Collections.shuffle(files);

        files.stream()
                .filter(f -> f.getAbsolutePath().endsWith("xml.gz"))
                .limit(MAX_FILES_TO_ANALYSE)
                .forEach(file -> {
                    System.out.println("Processing " + file.getName());
                    try (FileInputStream fileInputStream = new FileInputStream(file);
                         GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream, 65536)) {
                        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                        Document document = builder.parse(gzipInputStream);
                        analyse(document);
                    } catch (IOException | SAXException | ParserConfigurationException e) {
                        throw new RuntimeException(e);
                    }
                });
        medlineCitationAnalyser.finish();
    }

    private void analyse(Document document) {
        NodeList citationNodes = document.getElementsByTagName("MedlineCitation");
        for (int i = 0; i < citationNodes.getLength(); i++) {
            Node citation = citationNodes.item(i);
            medlineCitationAnalyser.analyse(citation);
        }
    }

    private void createDatabase(String server, String schema, String user, String password, String dateSourceType, String createSchema) {
        ConnectionWrapper connectionWrapper = new ConnectionWrapper(server, user, password, DbType.valueOf(dateSourceType.toUpperCase()));
        if (createSchema.equalsIgnoreCase("true"))
            connectionWrapper.createDatabase(schema);
        connectionWrapper.use(schema);
        System.out.println("Creating tables");
        medlineCitationAnalyser.createTables(connectionWrapper);
        PmidToDate.createTable(connectionWrapper);
        connectionWrapper.close();
        System.out.println("Finished creating table structure");
    }
}
