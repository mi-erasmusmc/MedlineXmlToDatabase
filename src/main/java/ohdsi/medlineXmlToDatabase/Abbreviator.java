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

import ohdsi.utilities.collections.Pair;

import java.util.List;

/**
 * Abbreviates some table names so they can fit in the DB
 *
 * @author MSCHUEMI
 */
public class Abbreviator {

    public static final List<Pair<String, String>> termToAbbr = List.of(
            new Pair<>("medlinecitation", "medcit"),
            new Pair<>("article", "art"),
            new Pair<>("investigator", "inv"),
            new Pair<>("affiliation", "aff"),
            new Pair<>("databank", "db"));

    public static String abbreviate(String name) {
        name = name.toLowerCase();
        for (Pair<String, String> pair : termToAbbr)
            name = name.replace(pair.getItem1(), pair.getItem2());
        return name;
    }

    public static String unAbbreviate(String name) {
        name = name.toLowerCase();
        for (Pair<String, String> pair : termToAbbr)
            name = name.replace(pair.getItem2(), pair.getItem1());
        return name;
    }
}
