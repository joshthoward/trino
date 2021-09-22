/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet.writer;

import io.airlift.testing.TempFile;
import org.apache.parquet.VersionParser;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.singletonList;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriter
{
    @Test
    public void testCreatedByIsParsable()
            throws VersionParser.VersionParseException, IOException
    {
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(singletonList(BOOLEAN), singletonList("a_boolean"));
        ParquetWriter parquetWriter = new ParquetWriter(
                new FileOutputStream(new TempFile().file()),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                ParquetWriterOptions.builder().build(),
                UNCOMPRESSED,
                "test-version");

        VersionParser.ParsedVersion version = VersionParser.parse(parquetWriter.createdBy);
        assertThat(version).isNotNull();
        assertThat(version.application).isEqualTo("Trino");
        assertThat(version.version).isEqualTo("test-version");
    }
}
