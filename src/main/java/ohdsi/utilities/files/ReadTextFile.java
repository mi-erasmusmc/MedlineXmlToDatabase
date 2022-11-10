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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ReadTextFile implements Iterable<String> {
    protected BufferedReader bufferedReader;
    private boolean EOF = false;


    public ReadTextFile(String filename) {
        try {
            FileInputStream inputStream = new FileInputStream(filename);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Iterator<String> iterator() {
        return new TextFileIterator();
    }

    private class TextFileIterator implements Iterator<String> {
        private String buffer;

        public TextFileIterator() {
            try {
                buffer = bufferedReader.readLine();
                if (buffer == null) {
                    EOF = true;
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        public boolean hasNext() {
            return !EOF;
        }

        public String next() {
            String result = buffer;
            try {
                buffer = bufferedReader.readLine();
                if (buffer == null) {
                    EOF = true;
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return result;
        }

        public void remove() {
            // not implemented
        }

    }
}
