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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * Main parser class. Here's where we iterate over all xml.gz files
 *
 * @author MSCHUEMI
 */
public class MedlineParserMain {

    private static final Logger log = LogManager.getLogger(MedlineParserMain.class.getName());


    public static void main(String[] args) {
        log.info("Starting MedLineParserMain");
        IniFile iniFile = new IniFile(args[0]);

        MedlineParserMain main = new MedlineParserMain();
        ConnectionWrapper connectionWrapper = new ConnectionWrapper(iniFile.get("SERVER"), iniFile.get("USER"), iniFile.get("PASSWORD"), DbType.valueOf(iniFile.get("DATA_SOURCE_TYPE").toUpperCase()));
        connectionWrapper.use(iniFile.get("SCHEMA"));
        main.parseFolder(iniFile.get("XML_FOLDER"), connectionWrapper, iniFile.get("SCHEMA"), iniFile.get("BASELINE"));
        log.info("Done!");
    }

    private void parseFolder(String folder, ConnectionWrapper connectionWrapper, String schema, String baseLine) {
        File[] files = new File(folder).listFiles();

        if (files == null || files.length == 0) {
            log.warn("Did not find any files to parse");
            return;
        }
        log.info("Parsing {} files", files.length);

        boolean updateFiles = !"BASELINE".equalsIgnoreCase(baseLine);
        log.info("Overwrite existing records: {}", updateFiles);

        SAXReader reader = new SAXReader();
        PmidToDate pmidToDate = new PmidToDate(connectionWrapper);
        MedlineCitationParser medlineCitationParser = new MedlineCitationParser(connectionWrapper, schema);

        Arrays.stream(Objects.requireNonNull(files))
                .filter(file -> file.getAbsolutePath().endsWith("xml.gz"))
                .sorted(Comparator.comparing(File::getName))
                .forEach(f -> {
                    long start = System.currentTimeMillis();
                    log.info("Processing {}", f.getName());
                    try (FileInputStream fileInputStream = new FileInputStream(f);
                         GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
                        Document document = reader.read(gzipInputStream);
                        log.info("Unzipped file into 'document'");
                        analyse(document, connectionWrapper, pmidToDate, medlineCitationParser, updateFiles);
                    } catch (IOException | DocumentException e) {
                        e.printStackTrace();
                    }
                    log.info("Completed {} in {} seconds", f.getName(), (System.currentTimeMillis() - start) / 1000);
                });
    }


    private void analyse(Document document, ConnectionWrapper connectionWrapper, PmidToDate pmidToDate, MedlineCitationParser medlineCitationParser, boolean updateFiles) {
        log.info("Loading citations");
        AtomicInteger i = new AtomicInteger();
        connectionWrapper.setBatchMode(true);
        document.getRootElement()
                .elementIterator("PubmedArticle")
                .forEachRemaining(element -> {
                    Node citation = element.selectSingleNode("./MedlineCitation");
                    medlineCitationParser.parseAndInjectIntoDB(citation, updateFiles);
                    pmidToDate.insertDates(citation, updateFiles);
                    if (i.get() % 100 == 0) {
                        try {
                            connectionWrapper.setBatchMode(false);
                        } catch (Exception e) {
                            log.error("Problem inserting batch into to DB for citations {} to {}", i.get() - 100, i);
                            log.error(e.getMessage());
                            e.printStackTrace();
                        }
                        connectionWrapper.setBatchMode(true);
                    }
                    i.getAndIncrement();
                });
        connectionWrapper.setBatchMode(false);
        if (updateFiles) {
            deleteCitations(document, connectionWrapper, medlineCitationParser);
        }
    }

    private void deleteCitations(Document document, ConnectionWrapper connectionWrapper, MedlineCitationParser medlineCitationParser) {
        List<Node> toBeDeleted = document.selectNodes("/PubmedArticleSet/DeleteCitation/PMID");
        if (!toBeDeleted.isEmpty()) {
            log.info("Deleting {} citations", toBeDeleted.size());
            connectionWrapper.setBatchMode(true);
            toBeDeleted.forEach(medlineCitationParser::delete);
            connectionWrapper.setBatchMode(false);
        }
    }
}
