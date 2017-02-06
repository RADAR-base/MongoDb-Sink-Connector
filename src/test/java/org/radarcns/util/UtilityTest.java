/*
 *  Copyright 2016 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDouble;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

public class UtilityTest {
    @Test
    public void intervalKeyToMongo() {
        SchemaBuilder sb = SchemaBuilder.struct();
        sb.field("userID", Schema.STRING_SCHEMA);
        sb.field("sourceID", Schema.STRING_SCHEMA);
        sb.field("start", Schema.INT64_SCHEMA);
        sb.field("end", Schema.INT64_SCHEMA);
        Schema schema = sb.schema();
        Struct struct = new Struct(schema);
        struct.put("userID", "myUser");
        struct.put("sourceID", "mySource");
        struct.put("start", 1000L);
        struct.put("end", 2000L);
        assertEquals("myUser-mySource-1000-2000", Utility.intervalKeyToMongoKey(struct));
    }

    @Test
    public void extractQuartile() {
        assertThat(Utility.extractQuartile(Arrays.asList(0.1, 0.2, 0.3)),
                contains(new Document("25", new BsonDouble(0.1)),
                        new Document("50", new BsonDouble(0.2)),
                        new Document("75", new BsonDouble(0.3))));
    }
}
