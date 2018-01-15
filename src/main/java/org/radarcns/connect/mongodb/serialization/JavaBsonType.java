/*
 * Copyright 2017 The Hyve and King's College London
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

package org.radarcns.connect.mongodb.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Java BSON type enum
 */
public enum JavaBsonType {
    LIST {
        public BsonValue toBson(Object value) {
            List<?> list = (List<?>) value;
            BsonArray array = new BsonArray();
            for (Object o : list) {
                array.add(objectToBson(o));
            }
            return array;
        }

        public String toString(Object value) {
            List<?> list = (List<?>) value;
            List<String> stringList = new ArrayList<>(list.size());
            for (Object o : list) {
                stringList.add(objectToString(o));
            }
            return "[" + String.join(",", stringList) + "]";
        }
    },
    MAP {
        public BsonValue toBson(Object record) {
            Map<?, ?> map = (Map) record;
            BsonDocument doc = new BsonDocument();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = objectToString(entry.getKey());
                Object value = entry.getValue();
                if (key.startsWith("time")) {
                    BsonDateTime dateTime = toDateTime(value);
                    if (dateTime != null) {
                        doc.put(key, dateTime);
                        continue;
                    }
                }
                doc.put(key, objectToBson(value));
            }
            return doc;
        }

        public String toString(Object value) {
            Map<?, ?> map = (Map) value;
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                stringMap.put(
                        objectToString(entry.getKey()),
                        objectToString(entry.getValue()));
            }
            List<String> sortedKeys = new ArrayList<>(stringMap.keySet());
            Collections.sort(sortedKeys);
            List<String> entryStrings = new ArrayList<>(stringMap.size());
            for (String key : sortedKeys) {
                entryStrings.add(key + ":" + stringMap.get(key));
            }
            return "{" + String.join(",", entryStrings) + "}";
        }
    },
    STRUCT {
        public BsonValue toBson(Object record) {
            Struct struct = (Struct) record;
            List<Field> fields = struct.schema().fields();
            BsonDocument doc = new BsonDocument();
            for (Field field : fields) {
                String key = field.name();
                Object value = struct.get(field);
                if (key.startsWith("time")) {
                    BsonDateTime dateTime = toDateTime(value);
                    if (dateTime != null) {
                        doc.put(key, dateTime);
                        continue;
                    }
                }
                doc.put(key, objectToBson(value));
            }
            return doc;
        }

        public String toString(Object value) {
            Struct struct = (Struct) value;
            List<Field> fields = struct.schema().fields();
            List<String> strings = new ArrayList<>(fields.size());
            for (Field field : fields) {
                strings.add(objectToString(struct.get(field)));
            }
            return "{" + String.join("-", strings) + "}";
        }
    },
    BYTE_ARRAY {
        private final char[] hexArray = "0123456789ABCDEF".toCharArray();

        public BsonValue toBson(Object value) {
            return new BsonBinary((byte[]) value);
        }

        public String toString(Object value) {
            byte[] bytes = (byte[]) value;
            char[] hexChars = new char[bytes.length * 2 + 2];
            // prepend "0x"
            hexChars[0] = '0';
            hexChars[1] = 'x';
            for (int j = 0; j < bytes.length; j++) {
                int pair = bytes[j] & 0xFF;
                hexChars[j * 2 + 2] = hexArray[pair >>> 4];
                hexChars[j * 2 + 3] = hexArray[pair & 0x0F];
            }
            return new String(hexChars);
        }
    },
    BYTE {
        public BsonValue toBson(Object value) {
            return new BsonInt32(((Number) value).intValue());
        }
    },
    SHORT {
        public BsonValue toBson(Object value) {
            return new BsonInt32(((Number) value).intValue());
        }
    },
    INTEGER {
        public BsonValue toBson(Object value) {
            return new BsonInt32(((Number) value).intValue());
        }
    },
    LONG {
        public BsonValue toBson(Object value) {
            return new BsonInt64((Long) value);
        }
    },
    BOOLEAN {
        public BsonValue toBson(Object value) {
            return new BsonBoolean((Boolean) value);
        }
    },
    FLOAT {
        public BsonValue toBson(Object value) {
            return new BsonDouble(floatToDouble((Float) value));
        }
    },
    DOUBLE {
        public BsonValue toBson(Object value) {
            return new BsonDouble((Double) value);
        }
    },
    STRING {
        public BsonValue toBson(Object value) {
            return new BsonString((String) value);
        }
    };

    public static final List<String> TYPE_NAMES = Arrays.stream(JavaBsonType.values())
            .map(Object::toString)
            .collect(Collectors.toList());

    abstract BsonValue toBson(Object value);

    public String toString(Object value) {
        return value.toString();
    }

    public static JavaBsonType getType(Object o) {
        String simpleName = o.getClass().getSimpleName().toUpperCase();

        if (TYPE_NAMES.contains(simpleName)) {
            return JavaBsonType.valueOf(simpleName);
        } else if (o instanceof byte[]) {
            return JavaBsonType.BYTE_ARRAY;
        } else if (o instanceof Map) {
            return JavaBsonType.MAP;
        } else if (o instanceof List) {
            return JavaBsonType.LIST;
        }

        throw new DataException("Data type " + o.getClass() + " not recognized");
    }

    /** Convert given record value to BSON. */
    public static BsonValue objectToBson(Object record) throws DataException {
        if (record == null) {
            return new BsonNull();
        }

        return getType(record).toBson(record);
    }

    public static String objectToString(Object o) {
        if (o == null) {
            return "null";
        }

        return getType(o).toString(o);
    }

    /** Stay closer to apparent value than to actual value. */
    private static double floatToDouble(float value) {
        return Double.parseDouble(Float.toString(value));
    }

    /**
     * Try to convert to BsonDateTime.
     *
     * @param obj raw value
     * @return BsonDateTime, or null if unsuccessful
     */
    private static BsonDateTime toDateTime(Object obj) {
        if (obj instanceof Long) {
            return new BsonDateTime((Long)obj);
        } else if (obj instanceof Double) {
            return new BsonDateTime((long)(1000d * (Double) obj));
        } else {
            return null;
        }
    }
}
