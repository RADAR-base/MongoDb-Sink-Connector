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
import org.junit.Test;

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
        Struct s = new Struct(schema);
        s.put("userID", "myUser");
        s.put("sourceID", "mySource");
        s.put("start", 1000L);
        s.put("end", 2000L);
        assertEquals("myUser-mySource-1000-2000", Utility.intervalKeyToMongoKey(s));
    }
}
