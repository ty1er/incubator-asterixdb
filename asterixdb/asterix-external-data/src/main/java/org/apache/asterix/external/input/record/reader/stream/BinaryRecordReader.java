/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.input.record.reader.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BinaryRecordReader extends ByteStreamRecordReader {

    private int recordSize;
    private static final List<String> recordReaderFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_BINARY));
    private static final String REQUIRED_CONFIGS = ExternalDataConstants.KEY_RECORD_SIZE;

    @Override
    public void configure(AsterixInputStream stream, Map<String, String> config) throws HyracksDataException {
        super.configure(stream);
        String recSizeString = config.get(ExternalDataConstants.KEY_RECORD_SIZE);
        if (recSizeString != null) {
            recordSize = Integer.parseInt(recSizeString);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        int readLength = 0;
        record.reset();
        while (true) {
            boolean fullRecord = false;
            if (done) {
                return false;
            }
            int startPosn = bufferPosn; //starting from where we left off the last time
            if (bufferPosn >= bufferLength) {
                startPosn = 0;
                bufferPosn -= bufferLength;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength <= 0) {
                    if (readLength > 0) {
                        record.endRecord();
                        return true;
                    }
                    close();
                    return false; //EOF
                }
                readLength = bufferPosn;
                fullRecord = true;
            }
            if (readLength == 0) {
                bufferPosn += recordSize;
                if (bufferPosn <= bufferLength) {
                    readLength = recordSize;
                    fullRecord = true;
                } else {
                    readLength = bufferLength - (bufferPosn - recordSize);
                }
            }
            record.append(inputBuffer, startPosn, readLength);
            if (fullRecord)
                break;

        }
        record.endRecord();
        return true;
    }

    @Override
    public List<String> getRecordReaderFormats() {
        return recordReaderFormats;
    }

    @Override
    public String getRequiredConfigs() {
        return REQUIRED_CONFIGS;
    }
}
