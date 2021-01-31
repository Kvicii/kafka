/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class SimpleRecordsMessageData implements ApiMessage {
    String topic;
    BaseRecords recordSet;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("topic", Type.STRING, "The topic name."),
            new Field("record_set", Type.RECORDS, "The record data.")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("topic", Type.COMPACT_STRING, "The topic name."),
            new Field("record_set", Type.COMPACT_RECORDS, "The record data."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 1;
    
    public SimpleRecordsMessageData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public SimpleRecordsMessageData() {
        this.topic = "";
        this.recordSet = null;
    }
    
    @Override
    public short apiKey() {
        return -1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 1;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        {
            int length;
            if (_version >= 1) {
                length = _readable.readUnsignedVarint() - 1;
            } else {
                length = _readable.readShort();
            }
            if (length < 0) {
                throw new RuntimeException("non-nullable field topic was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field topic had invalid length " + length);
            } else {
                this.topic = _readable.readString(length);
            }
        }
        {
            int length;
            if (_version >= 1) {
                length = _readable.readUnsignedVarint() - 1;
            } else {
                length = _readable.readInt();
            }
            if (length < 0) {
                this.recordSet = null;
            } else {
                this.recordSet = _readable.readRecords(length);
            }
        }
        this._unknownTaggedFields = null;
        if (_version >= 1) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = _cache.getSerializedValue(topic);
            if (_version >= 1) {
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
            } else {
                _writable.writeShort((short) _stringBytes.length);
            }
            _writable.writeByteArray(_stringBytes);
        }
        if (recordSet == null) {
            if (_version >= 1) {
                _writable.writeUnsignedVarint(0);
            } else {
                _writable.writeInt(-1);
            }
        } else {
            if (_version >= 1) {
                _writable.writeUnsignedVarint(recordSet.sizeInBytes() + 1);
            } else {
                _writable.writeInt(recordSet.sizeInBytes());
            }
            _writable.writeRecords(recordSet);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 1) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = topic.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'topic' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(topic, _stringBytes);
            if (_version >= 1) {
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            } else {
                _size.addBytes(_stringBytes.length + 2);
            }
        }
        if (recordSet == null) {
            if (_version >= 1) {
                _size.addBytes(1);
            } else {
                _size.addBytes(4);
            }
        } else {
            _size.addZeroCopyBytes(recordSet.sizeInBytes());
            if (_version >= 1) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(recordSet.sizeInBytes() + 1));
            } else {
                _size.addBytes(4);
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_version >= 1) {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SimpleRecordsMessageData)) return false;
        SimpleRecordsMessageData other = (SimpleRecordsMessageData) obj;
        if (this.topic == null) {
            if (other.topic != null) return false;
        } else {
            if (!this.topic.equals(other.topic)) return false;
        }
        if (!Objects.equals(this.recordSet, other.recordSet)) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
        hashCode = 31 * hashCode + Objects.hashCode(recordSet);
        return hashCode;
    }
    
    @Override
    public SimpleRecordsMessageData duplicate() {
        SimpleRecordsMessageData _duplicate = new SimpleRecordsMessageData();
        _duplicate.topic = topic;
        if (recordSet == null) {
            _duplicate.recordSet = null;
        } else {
            _duplicate.recordSet = MemoryRecords.readableRecords(((MemoryRecords) recordSet).buffer().duplicate());
        }
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "SimpleRecordsMessageData("
            + "topic=" + ((topic == null) ? "null" : "'" + topic.toString() + "'")
            + ", recordSet=" + recordSet
            + ")";
    }
    
    public String topic() {
        return this.topic;
    }
    
    public BaseRecords recordSet() {
        return this.recordSet;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public SimpleRecordsMessageData setTopic(String v) {
        this.topic = v;
        return this;
    }
    
    public SimpleRecordsMessageData setRecordSet(BaseRecords v) {
        this.recordSet = v;
        return this;
    }
}
