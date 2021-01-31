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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class SimpleExampleMessageData implements ApiMessage {
    Uuid processId;
    List<Integer> myTaggedIntArray;
    String myNullableString;
    short myInt16;
    double myFloat64;
    String myString;
    byte[] myBytes;
    Uuid taggedUuid;
    long taggedLong;
    ByteBuffer zeroCopyByteBuffer;
    ByteBuffer nullableZeroCopyByteBuffer;
    MyStruct myStruct;
    TaggedStruct myTaggedStruct;
    TestCommonStruct myCommonStruct;
    TestCommonStruct myOtherCommonStruct;
    int myUint16;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("my_common_struct", TestCommonStruct.SCHEMA_0, ""),
            new Field("my_other_common_struct", TestCommonStruct.SCHEMA_0, "")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("process_id", Type.UUID, ""),
            new Field("zero_copy_byte_buffer", Type.COMPACT_BYTES, ""),
            new Field("nullable_zero_copy_byte_buffer", Type.COMPACT_NULLABLE_BYTES, ""),
            new Field("my_common_struct", TestCommonStruct.SCHEMA_1, ""),
            new Field("my_other_common_struct", TestCommonStruct.SCHEMA_1, ""),
            new Field("my_uint16", Type.UINT16, ""),
            TaggedFieldsSection.of(
                0, new Field("my_tagged_int_array", new CompactArrayOf(Type.INT32), ""),
                1, new Field("my_nullable_string", Type.COMPACT_NULLABLE_STRING, ""),
                2, new Field("my_int16", Type.INT16, ""),
                3, new Field("my_float64", Type.FLOAT64, ""),
                4, new Field("my_string", Type.COMPACT_STRING, ""),
                5, new Field("my_bytes", Type.COMPACT_NULLABLE_BYTES, ""),
                6, new Field("tagged_uuid", Type.UUID, ""),
                7, new Field("tagged_long", Type.INT64, "")
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("process_id", Type.UUID, ""),
            new Field("my_struct", MyStruct.SCHEMA_2, "Test Struct field"),
            new Field("my_common_struct", TestCommonStruct.SCHEMA_1, ""),
            new Field("my_other_common_struct", TestCommonStruct.SCHEMA_1, ""),
            new Field("my_uint16", Type.UINT16, ""),
            TaggedFieldsSection.of(
                0, new Field("my_tagged_int_array", new CompactArrayOf(Type.INT32), ""),
                1, new Field("my_nullable_string", Type.COMPACT_NULLABLE_STRING, ""),
                2, new Field("my_int16", Type.INT16, ""),
                3, new Field("my_float64", Type.FLOAT64, ""),
                4, new Field("my_string", Type.COMPACT_STRING, ""),
                5, new Field("my_bytes", Type.COMPACT_NULLABLE_BYTES, ""),
                6, new Field("tagged_uuid", Type.UUID, ""),
                7, new Field("tagged_long", Type.INT64, ""),
                8, new Field("my_tagged_struct", TaggedStruct.SCHEMA_2, "Test Tagged Struct field")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 2;
    
    public SimpleExampleMessageData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public SimpleExampleMessageData() {
        this.processId = Uuid.ZERO_UUID;
        this.myTaggedIntArray = new ArrayList<Integer>(0);
        this.myNullableString = null;
        this.myInt16 = (short) 123;
        this.myFloat64 = Double.parseDouble("12.34");
        this.myString = "";
        this.myBytes = Bytes.EMPTY;
        this.taggedUuid = Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A");
        this.taggedLong = 0xcafcacafcacafcaL;
        this.zeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
        this.nullableZeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
        this.myStruct = new MyStruct();
        this.myTaggedStruct = new TaggedStruct();
        this.myCommonStruct = new TestCommonStruct();
        this.myOtherCommonStruct = new TestCommonStruct();
        this.myUint16 = 33000;
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
        return 2;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        if (_version >= 1) {
            this.processId = _readable.readUuid();
        } else {
            this.processId = Uuid.ZERO_UUID;
        }
        {
            this.myTaggedIntArray = new ArrayList<Integer>(0);
        }
        {
            this.myNullableString = null;
        }
        this.myInt16 = (short) 123;
        this.myFloat64 = Double.parseDouble("12.34");
        {
            this.myString = "";
        }
        {
            this.myBytes = Bytes.EMPTY;
        }
        this.taggedUuid = Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A");
        this.taggedLong = 0xcafcacafcacafcaL;
        if ((_version >= 1) && (_version <= 1)) {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                throw new RuntimeException("non-nullable field zeroCopyByteBuffer was serialized as null");
            } else {
                this.zeroCopyByteBuffer = _readable.readByteBuffer(length);
            }
        } else {
            this.zeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
        }
        if ((_version >= 1) && (_version <= 1)) {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                this.nullableZeroCopyByteBuffer = null;
            } else {
                this.nullableZeroCopyByteBuffer = _readable.readByteBuffer(length);
            }
        } else {
            this.nullableZeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
        }
        if (_version >= 2) {
            this.myStruct = new MyStruct(_readable, _version);
        } else {
            this.myStruct = new MyStruct();
        }
        {
            this.myTaggedStruct = new TaggedStruct();
        }
        {
            this.myCommonStruct = new TestCommonStruct(_readable, _version);
        }
        {
            this.myOtherCommonStruct = new TestCommonStruct(_readable, _version);
        }
        if (_version >= 1) {
            this.myUint16 = _readable.readUnsignedShort();
        } else {
            this.myUint16 = 33000;
        }
        this._unknownTaggedFields = null;
        if (_version >= 1) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    case 0: {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            throw new RuntimeException("non-nullable field myTaggedIntArray was serialized as null");
                        } else {
                            ArrayList<Integer> newCollection = new ArrayList<Integer>(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(_readable.readInt());
                            }
                            this.myTaggedIntArray = newCollection;
                        }
                        break;
                    }
                    case 1: {
                        int length;
                        length = _readable.readUnsignedVarint() - 1;
                        if (length < 0) {
                            this.myNullableString = null;
                        } else if (length > 0x7fff) {
                            throw new RuntimeException("string field myNullableString had invalid length " + length);
                        } else {
                            this.myNullableString = _readable.readString(length);
                        }
                        break;
                    }
                    case 2: {
                        this.myInt16 = _readable.readShort();
                        break;
                    }
                    case 3: {
                        this.myFloat64 = _readable.readDouble();
                        break;
                    }
                    case 4: {
                        int length;
                        length = _readable.readUnsignedVarint() - 1;
                        if (length < 0) {
                            throw new RuntimeException("non-nullable field myString was serialized as null");
                        } else if (length > 0x7fff) {
                            throw new RuntimeException("string field myString had invalid length " + length);
                        } else {
                            this.myString = _readable.readString(length);
                        }
                        break;
                    }
                    case 5: {
                        int length;
                        length = _readable.readUnsignedVarint() - 1;
                        if (length < 0) {
                            this.myBytes = null;
                        } else {
                            byte[] newBytes = new byte[length];
                            _readable.readArray(newBytes);
                            this.myBytes = newBytes;
                        }
                        break;
                    }
                    case 6: {
                        this.taggedUuid = _readable.readUuid();
                        break;
                    }
                    case 7: {
                        this.taggedLong = _readable.readLong();
                        break;
                    }
                    case 8: {
                        if (_version >= 2) {
                            this.myTaggedStruct = new TaggedStruct(_readable, _version);
                            break;
                        } else {
                            throw new RuntimeException("Tag 8 is not valid for version " + _version);
                        }
                    }
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
        if (_version >= 1) {
            _writable.writeUuid(processId);
        } else {
            if (this.processId != Uuid.ZERO_UUID) {
                throw new UnsupportedVersionException("Attempted to write a non-default processId at version " + _version);
            }
        }
        if (_version >= 1) {
            if (!this.myTaggedIntArray.isEmpty()) {
                _numTaggedFields++;
            }
        } else {
            if (!this.myTaggedIntArray.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default myTaggedIntArray at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.myNullableString != null) {
                _numTaggedFields++;
            }
        } else {
            if (this.myNullableString != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default myNullableString at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.myInt16 != (short) 123) {
                _numTaggedFields++;
            }
        } else {
            if (this.myInt16 != (short) 123) {
                throw new UnsupportedVersionException("Attempted to write a non-default myInt16 at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.myFloat64 != Double.parseDouble("12.34")) {
                _numTaggedFields++;
            }
        } else {
            if (this.myFloat64 != Double.parseDouble("12.34")) {
                throw new UnsupportedVersionException("Attempted to write a non-default myFloat64 at version " + _version);
            }
        }
        if (_version >= 1) {
            if (!this.myString.equals("")) {
                _numTaggedFields++;
            }
        } else {
            if (!this.myString.equals("")) {
                throw new UnsupportedVersionException("Attempted to write a non-default myString at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.myBytes == null || this.myBytes.length != 0) {
                _numTaggedFields++;
            }
        } else {
            if (this.myBytes == null || this.myBytes.length != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default myBytes at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.taggedUuid != Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A")) {
                _numTaggedFields++;
            }
        } else {
            if (this.taggedUuid != Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A")) {
                throw new UnsupportedVersionException("Attempted to write a non-default taggedUuid at version " + _version);
            }
        }
        if (_version >= 1) {
            if (this.taggedLong != 0xcafcacafcacafcaL) {
                _numTaggedFields++;
            }
        } else {
            if (this.taggedLong != 0xcafcacafcacafcaL) {
                throw new UnsupportedVersionException("Attempted to write a non-default taggedLong at version " + _version);
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            _writable.writeUnsignedVarint(zeroCopyByteBuffer.remaining() + 1);
            _writable.writeByteBuffer(zeroCopyByteBuffer);
        } else {
            if (this.zeroCopyByteBuffer.hasRemaining()) {
                throw new UnsupportedVersionException("Attempted to write a non-default zeroCopyByteBuffer at version " + _version);
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            if (nullableZeroCopyByteBuffer == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                _writable.writeUnsignedVarint(nullableZeroCopyByteBuffer.remaining() + 1);
                _writable.writeByteBuffer(nullableZeroCopyByteBuffer);
            }
        } else {
            if (this.nullableZeroCopyByteBuffer == null || this.nullableZeroCopyByteBuffer.remaining() > 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default nullableZeroCopyByteBuffer at version " + _version);
            }
        }
        if (_version >= 2) {
            myStruct.write(_writable, _cache, _version);
        } else {
            if (!this.myStruct.equals(new MyStruct())) {
                throw new UnsupportedVersionException("Attempted to write a non-default myStruct at version " + _version);
            }
        }
        if (_version >= 2) {
            if (!this.myTaggedStruct.equals(new TaggedStruct())) {
                _numTaggedFields++;
            }
        } else {
            if (!this.myTaggedStruct.equals(new TaggedStruct())) {
                throw new UnsupportedVersionException("Attempted to write a non-default myTaggedStruct at version " + _version);
            }
        }
        myCommonStruct.write(_writable, _cache, _version);
        myOtherCommonStruct.write(_writable, _cache, _version);
        if (_version >= 1) {
            _writable.writeUnsignedShort(myUint16);
        } else {
            if (this.myUint16 != 33000) {
                throw new UnsupportedVersionException("Attempted to write a non-default myUint16 at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 1) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            {
                if (!this.myTaggedIntArray.isEmpty()) {
                    _writable.writeUnsignedVarint(0);
                    _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.myTaggedIntArray));
                    _writable.writeUnsignedVarint(myTaggedIntArray.size() + 1);
                    for (Integer myTaggedIntArrayElement : myTaggedIntArray) {
                        _writable.writeInt(myTaggedIntArrayElement);
                    }
                }
            }
            if (myNullableString != null) {
                _writable.writeUnsignedVarint(1);
                byte[] _stringBytes = _cache.getSerializedValue(this.myNullableString);
                _writable.writeUnsignedVarint(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            {
                if (this.myInt16 != (short) 123) {
                    _writable.writeUnsignedVarint(2);
                    _writable.writeUnsignedVarint(2);
                    _writable.writeShort(myInt16);
                }
            }
            {
                if (this.myFloat64 != Double.parseDouble("12.34")) {
                    _writable.writeUnsignedVarint(3);
                    _writable.writeUnsignedVarint(8);
                    _writable.writeDouble(myFloat64);
                }
            }
            {
                if (!this.myString.equals("")) {
                    _writable.writeUnsignedVarint(4);
                    byte[] _stringBytes = _cache.getSerializedValue(this.myString);
                    _writable.writeUnsignedVarint(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    _writable.writeByteArray(_stringBytes);
                }
            }
            if (myBytes == null) {
                _writable.writeUnsignedVarint(5);
                _writable.writeUnsignedVarint(1);
                _writable.writeUnsignedVarint(0);
            } else {
                if (this.myBytes.length != 0) {
                    _writable.writeUnsignedVarint(5);
                    _writable.writeUnsignedVarint(this.myBytes.length + ByteUtils.sizeOfUnsignedVarint(this.myBytes.length + 1));
                    _writable.writeUnsignedVarint(this.myBytes.length + 1);
                    _writable.writeByteArray(this.myBytes);
                }
            }
            {
                if (this.taggedUuid != Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A")) {
                    _writable.writeUnsignedVarint(6);
                    _writable.writeUnsignedVarint(16);
                    _writable.writeUuid(taggedUuid);
                }
            }
            {
                if (this.taggedLong != 0xcafcacafcacafcaL) {
                    _writable.writeUnsignedVarint(7);
                    _writable.writeUnsignedVarint(8);
                    _writable.writeLong(taggedLong);
                }
            }
            {
                if (!this.myTaggedStruct.equals(new TaggedStruct())) {
                    _writable.writeUnsignedVarint(8);
                    _writable.writeUnsignedVarint(this.myTaggedStruct.size(_cache, _version));
                    myTaggedStruct.write(_writable, _cache, _version);
                }
            }
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
        if (_version >= 1) {
            _size.addBytes(16);
        }
        if (_version >= 1) {
            {
                if (!this.myTaggedIntArray.isEmpty()) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    int _sizeBeforeArray = _size.totalSize();
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(myTaggedIntArray.size() + 1));
                    _size.addBytes(myTaggedIntArray.size() * 4);
                    int _arraySize = _size.totalSize() - _sizeBeforeArray;
                    _cache.setArraySizeInBytes(myTaggedIntArray, _arraySize);
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
                }
            }
        }
        if (_version >= 1) {
            if (myNullableString == null) {
            } else {
                _numTaggedFields++;
                _size.addBytes(1);
                byte[] _stringBytes = myNullableString.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'myNullableString' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(myNullableString, _stringBytes);
                int _stringPrefixSize = ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
                _size.addBytes(_stringBytes.length + _stringPrefixSize + ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize));
            }
        }
        if (_version >= 1) {
            if (this.myInt16 != (short) 123) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(2);
            }
        }
        if (_version >= 1) {
            if (this.myFloat64 != Double.parseDouble("12.34")) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(8);
            }
        }
        if (_version >= 1) {
            {
                if (!this.myString.equals("")) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    byte[] _stringBytes = myString.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'myString' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(myString, _stringBytes);
                    int _stringPrefixSize = ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
                    _size.addBytes(_stringBytes.length + _stringPrefixSize + ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize));
                }
            }
        }
        if (_version >= 1) {
            if (myBytes == null) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(1);
            } else {
                if (this.myBytes.length != 0) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    int _sizeBeforeBytes = _size.totalSize();
                    _size.addBytes(myBytes.length);
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(myBytes.length + 1));
                    int _bytesSize = _size.totalSize() - _sizeBeforeBytes;
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_bytesSize));
                }
            }
        }
        if (_version >= 1) {
            if (this.taggedUuid != Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A")) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(16);
            }
        }
        if (_version >= 1) {
            if (this.taggedLong != 0xcafcacafcacafcaL) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(8);
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            {
                _size.addZeroCopyBytes(zeroCopyByteBuffer.remaining());
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(zeroCopyByteBuffer.remaining() + 1));
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            if (nullableZeroCopyByteBuffer == null) {
                _size.addBytes(1);
            } else {
                _size.addZeroCopyBytes(nullableZeroCopyByteBuffer.remaining());
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(nullableZeroCopyByteBuffer.remaining() + 1));
            }
        }
        if (_version >= 2) {
            {
                int _sizeBeforeStruct = _size.totalSize();
                this.myStruct.addSize(_size, _cache, _version);
                int _structSize = _size.totalSize() - _sizeBeforeStruct;
            }
        }
        if (_version >= 2) {
            {
                if (!this.myTaggedStruct.equals(new TaggedStruct())) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    int _sizeBeforeStruct = _size.totalSize();
                    this.myTaggedStruct.addSize(_size, _cache, _version);
                    int _structSize = _size.totalSize() - _sizeBeforeStruct;
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));
                }
            }
        }
        {
            int _sizeBeforeStruct = _size.totalSize();
            this.myCommonStruct.addSize(_size, _cache, _version);
            int _structSize = _size.totalSize() - _sizeBeforeStruct;
        }
        {
            int _sizeBeforeStruct = _size.totalSize();
            this.myOtherCommonStruct.addSize(_size, _cache, _version);
            int _structSize = _size.totalSize() - _sizeBeforeStruct;
        }
        if (_version >= 1) {
            _size.addBytes(2);
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
        if (!(obj instanceof SimpleExampleMessageData)) return false;
        SimpleExampleMessageData other = (SimpleExampleMessageData) obj;
        if (!this.processId.equals(other.processId)) return false;
        if (this.myTaggedIntArray == null) {
            if (other.myTaggedIntArray != null) return false;
        } else {
            if (!this.myTaggedIntArray.equals(other.myTaggedIntArray)) return false;
        }
        if (this.myNullableString == null) {
            if (other.myNullableString != null) return false;
        } else {
            if (!this.myNullableString.equals(other.myNullableString)) return false;
        }
        if (myInt16 != other.myInt16) return false;
        if (myFloat64 != other.myFloat64) return false;
        if (this.myString == null) {
            if (other.myString != null) return false;
        } else {
            if (!this.myString.equals(other.myString)) return false;
        }
        if (!Arrays.equals(this.myBytes, other.myBytes)) return false;
        if (!this.taggedUuid.equals(other.taggedUuid)) return false;
        if (taggedLong != other.taggedLong) return false;
        if (!Objects.equals(this.zeroCopyByteBuffer, other.zeroCopyByteBuffer)) return false;
        if (!Objects.equals(this.nullableZeroCopyByteBuffer, other.nullableZeroCopyByteBuffer)) return false;
        if (this.myStruct == null) {
            if (other.myStruct != null) return false;
        } else {
            if (!this.myStruct.equals(other.myStruct)) return false;
        }
        if (this.myTaggedStruct == null) {
            if (other.myTaggedStruct != null) return false;
        } else {
            if (!this.myTaggedStruct.equals(other.myTaggedStruct)) return false;
        }
        if (this.myCommonStruct == null) {
            if (other.myCommonStruct != null) return false;
        } else {
            if (!this.myCommonStruct.equals(other.myCommonStruct)) return false;
        }
        if (this.myOtherCommonStruct == null) {
            if (other.myOtherCommonStruct != null) return false;
        } else {
            if (!this.myOtherCommonStruct.equals(other.myOtherCommonStruct)) return false;
        }
        if (myUint16 != other.myUint16) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + processId.hashCode();
        hashCode = 31 * hashCode + (myTaggedIntArray == null ? 0 : myTaggedIntArray.hashCode());
        hashCode = 31 * hashCode + (myNullableString == null ? 0 : myNullableString.hashCode());
        hashCode = 31 * hashCode + myInt16;
        hashCode = 31 * hashCode + Double.hashCode(myFloat64);
        hashCode = 31 * hashCode + (myString == null ? 0 : myString.hashCode());
        hashCode = 31 * hashCode + Arrays.hashCode(myBytes);
        hashCode = 31 * hashCode + taggedUuid.hashCode();
        hashCode = 31 * hashCode + ((int) (taggedLong >> 32) ^ (int) taggedLong);
        hashCode = 31 * hashCode + Objects.hashCode(zeroCopyByteBuffer);
        hashCode = 31 * hashCode + Objects.hashCode(nullableZeroCopyByteBuffer);
        hashCode = 31 * hashCode + (myStruct == null ? 0 : myStruct.hashCode());
        hashCode = 31 * hashCode + (myTaggedStruct == null ? 0 : myTaggedStruct.hashCode());
        hashCode = 31 * hashCode + (myCommonStruct == null ? 0 : myCommonStruct.hashCode());
        hashCode = 31 * hashCode + (myOtherCommonStruct == null ? 0 : myOtherCommonStruct.hashCode());
        hashCode = 31 * hashCode + myUint16;
        return hashCode;
    }
    
    @Override
    public SimpleExampleMessageData duplicate() {
        SimpleExampleMessageData _duplicate = new SimpleExampleMessageData();
        _duplicate.processId = processId;
        ArrayList<Integer> newMyTaggedIntArray = new ArrayList<Integer>(myTaggedIntArray.size());
        for (Integer _element : myTaggedIntArray) {
            newMyTaggedIntArray.add(_element);
        }
        _duplicate.myTaggedIntArray = newMyTaggedIntArray;
        if (myNullableString == null) {
            _duplicate.myNullableString = null;
        } else {
            _duplicate.myNullableString = myNullableString;
        }
        _duplicate.myInt16 = myInt16;
        _duplicate.myFloat64 = myFloat64;
        _duplicate.myString = myString;
        if (myBytes == null) {
            _duplicate.myBytes = null;
        } else {
            _duplicate.myBytes = MessageUtil.duplicate(myBytes);
        }
        _duplicate.taggedUuid = taggedUuid;
        _duplicate.taggedLong = taggedLong;
        _duplicate.zeroCopyByteBuffer = zeroCopyByteBuffer.duplicate();
        if (nullableZeroCopyByteBuffer == null) {
            _duplicate.nullableZeroCopyByteBuffer = null;
        } else {
            _duplicate.nullableZeroCopyByteBuffer = nullableZeroCopyByteBuffer.duplicate();
        }
        _duplicate.myStruct = myStruct.duplicate();
        _duplicate.myTaggedStruct = myTaggedStruct.duplicate();
        _duplicate.myCommonStruct = myCommonStruct.duplicate();
        _duplicate.myOtherCommonStruct = myOtherCommonStruct.duplicate();
        _duplicate.myUint16 = myUint16;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "SimpleExampleMessageData("
            + "processId=" + processId.toString()
            + ", myTaggedIntArray=" + MessageUtil.deepToString(myTaggedIntArray.iterator())
            + ", myNullableString=" + ((myNullableString == null) ? "null" : "'" + myNullableString.toString() + "'")
            + ", myInt16=" + myInt16
            + ", myFloat64=" + myFloat64
            + ", myString=" + ((myString == null) ? "null" : "'" + myString.toString() + "'")
            + ", myBytes=" + Arrays.toString(myBytes)
            + ", taggedUuid=" + taggedUuid.toString()
            + ", taggedLong=" + taggedLong
            + ", zeroCopyByteBuffer=" + zeroCopyByteBuffer
            + ", nullableZeroCopyByteBuffer=" + nullableZeroCopyByteBuffer
            + ", myStruct=" + myStruct.toString()
            + ", myTaggedStruct=" + myTaggedStruct.toString()
            + ", myCommonStruct=" + myCommonStruct.toString()
            + ", myOtherCommonStruct=" + myOtherCommonStruct.toString()
            + ", myUint16=" + myUint16
            + ")";
    }
    
    public Uuid processId() {
        return this.processId;
    }
    
    public List<Integer> myTaggedIntArray() {
        return this.myTaggedIntArray;
    }
    
    public String myNullableString() {
        return this.myNullableString;
    }
    
    public short myInt16() {
        return this.myInt16;
    }
    
    public double myFloat64() {
        return this.myFloat64;
    }
    
    public String myString() {
        return this.myString;
    }
    
    public byte[] myBytes() {
        return this.myBytes;
    }
    
    public Uuid taggedUuid() {
        return this.taggedUuid;
    }
    
    public long taggedLong() {
        return this.taggedLong;
    }
    
    public ByteBuffer zeroCopyByteBuffer() {
        return this.zeroCopyByteBuffer;
    }
    
    public ByteBuffer nullableZeroCopyByteBuffer() {
        return this.nullableZeroCopyByteBuffer;
    }
    
    public MyStruct myStruct() {
        return this.myStruct;
    }
    
    public TaggedStruct myTaggedStruct() {
        return this.myTaggedStruct;
    }
    
    public TestCommonStruct myCommonStruct() {
        return this.myCommonStruct;
    }
    
    public TestCommonStruct myOtherCommonStruct() {
        return this.myOtherCommonStruct;
    }
    
    public int myUint16() {
        return this.myUint16;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public SimpleExampleMessageData setProcessId(Uuid v) {
        this.processId = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyTaggedIntArray(List<Integer> v) {
        this.myTaggedIntArray = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyNullableString(String v) {
        this.myNullableString = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyInt16(short v) {
        this.myInt16 = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyFloat64(double v) {
        this.myFloat64 = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyString(String v) {
        this.myString = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyBytes(byte[] v) {
        this.myBytes = v;
        return this;
    }
    
    public SimpleExampleMessageData setTaggedUuid(Uuid v) {
        this.taggedUuid = v;
        return this;
    }
    
    public SimpleExampleMessageData setTaggedLong(long v) {
        this.taggedLong = v;
        return this;
    }
    
    public SimpleExampleMessageData setZeroCopyByteBuffer(ByteBuffer v) {
        this.zeroCopyByteBuffer = v;
        return this;
    }
    
    public SimpleExampleMessageData setNullableZeroCopyByteBuffer(ByteBuffer v) {
        this.nullableZeroCopyByteBuffer = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyStruct(MyStruct v) {
        this.myStruct = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyTaggedStruct(TaggedStruct v) {
        this.myTaggedStruct = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyCommonStruct(TestCommonStruct v) {
        this.myCommonStruct = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyOtherCommonStruct(TestCommonStruct v) {
        this.myOtherCommonStruct = v;
        return this;
    }
    
    public SimpleExampleMessageData setMyUint16(int v) {
        if (v < 0 || v > 65535) {
            throw new RuntimeException("Invalid value " + v + "for unsigned short field.");
        }
        this.myUint16 = v;
        return this;
    }
    
    public static class MyStruct implements Message {
        int structId;
        List<StructArray> arrayInStruct;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("struct_id", Type.INT32, "Int field in struct"),
                new Field("array_in_struct", new CompactArrayOf(StructArray.SCHEMA_2), ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 2;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public MyStruct(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public MyStruct() {
            this.structId = 0;
            this.arrayInStruct = new ArrayList<StructArray>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MyStruct");
            }
            this.structId = _readable.readInt();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field arrayInStruct was serialized as null");
                } else {
                    ArrayList<StructArray> newCollection = new ArrayList<StructArray>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new StructArray(_readable, _version));
                    }
                    this.arrayInStruct = newCollection;
                }
            }
            this._unknownTaggedFields = null;
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
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MyStruct");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(structId);
            _writable.writeUnsignedVarint(arrayInStruct.size() + 1);
            for (StructArray arrayInStructElement : arrayInStruct) {
                arrayInStructElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of MyStruct");
            }
            _size.addBytes(4);
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(arrayInStruct.size() + 1));
                for (StructArray arrayInStructElement : arrayInStruct) {
                    arrayInStructElement.addSize(_size, _cache, _version);
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
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyStruct)) return false;
            MyStruct other = (MyStruct) obj;
            if (structId != other.structId) return false;
            if (this.arrayInStruct == null) {
                if (other.arrayInStruct != null) return false;
            } else {
                if (!this.arrayInStruct.equals(other.arrayInStruct)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + structId;
            hashCode = 31 * hashCode + (arrayInStruct == null ? 0 : arrayInStruct.hashCode());
            return hashCode;
        }
        
        @Override
        public MyStruct duplicate() {
            MyStruct _duplicate = new MyStruct();
            _duplicate.structId = structId;
            ArrayList<StructArray> newArrayInStruct = new ArrayList<StructArray>(arrayInStruct.size());
            for (StructArray _element : arrayInStruct) {
                newArrayInStruct.add(_element.duplicate());
            }
            _duplicate.arrayInStruct = newArrayInStruct;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "MyStruct("
                + "structId=" + structId
                + ", arrayInStruct=" + MessageUtil.deepToString(arrayInStruct.iterator())
                + ")";
        }
        
        public int structId() {
            return this.structId;
        }
        
        public List<StructArray> arrayInStruct() {
            return this.arrayInStruct;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public MyStruct setStructId(int v) {
            this.structId = v;
            return this;
        }
        
        public MyStruct setArrayInStruct(List<StructArray> v) {
            this.arrayInStruct = v;
            return this;
        }
    }
    
    public static class StructArray implements Message {
        int arrayFieldId;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("array_field_id", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 2;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public StructArray(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public StructArray() {
            this.arrayFieldId = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 2;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of StructArray");
            }
            this.arrayFieldId = _readable.readInt();
            this._unknownTaggedFields = null;
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
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(arrayFieldId);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of StructArray");
            }
            _size.addBytes(4);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StructArray)) return false;
            StructArray other = (StructArray) obj;
            if (arrayFieldId != other.arrayFieldId) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + arrayFieldId;
            return hashCode;
        }
        
        @Override
        public StructArray duplicate() {
            StructArray _duplicate = new StructArray();
            _duplicate.arrayFieldId = arrayFieldId;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "StructArray("
                + "arrayFieldId=" + arrayFieldId
                + ")";
        }
        
        public int arrayFieldId() {
            return this.arrayFieldId;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public StructArray setArrayFieldId(int v) {
            this.arrayFieldId = v;
            return this;
        }
    }
    
    public static class TaggedStruct implements Message {
        String structId;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("struct_id", Type.COMPACT_STRING, "String field in struct"),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 2;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public TaggedStruct(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TaggedStruct() {
            this.structId = "";
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TaggedStruct");
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field structId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field structId had invalid length " + length);
                } else {
                    this.structId = _readable.readString(length);
                }
            }
            this._unknownTaggedFields = null;
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
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of TaggedStruct");
            }
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(structId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TaggedStruct");
            }
            {
                byte[] _stringBytes = structId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'structId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(structId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TaggedStruct)) return false;
            TaggedStruct other = (TaggedStruct) obj;
            if (this.structId == null) {
                if (other.structId != null) return false;
            } else {
                if (!this.structId.equals(other.structId)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (structId == null ? 0 : structId.hashCode());
            return hashCode;
        }
        
        @Override
        public TaggedStruct duplicate() {
            TaggedStruct _duplicate = new TaggedStruct();
            _duplicate.structId = structId;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TaggedStruct("
                + "structId=" + ((structId == null) ? "null" : "'" + structId.toString() + "'")
                + ")";
        }
        
        public String structId() {
            return this.structId;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TaggedStruct setStructId(String v) {
            this.structId = v;
            return this;
        }
    }
    
    public static class TestCommonStruct implements Message {
        int foo;
        int bar;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("foo", Type.INT32, ""),
                new Field("bar", Type.INT32, "")
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("foo", Type.INT32, ""),
                new Field("bar", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public TestCommonStruct(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TestCommonStruct() {
            this.foo = 123;
            this.bar = 123;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 32767;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            this.foo = _readable.readInt();
            this.bar = _readable.readInt();
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
            _writable.writeInt(foo);
            _writable.writeInt(bar);
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
            _size.addBytes(4);
            _size.addBytes(4);
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
            if (!(obj instanceof TestCommonStruct)) return false;
            TestCommonStruct other = (TestCommonStruct) obj;
            if (foo != other.foo) return false;
            if (bar != other.bar) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + foo;
            hashCode = 31 * hashCode + bar;
            return hashCode;
        }
        
        @Override
        public TestCommonStruct duplicate() {
            TestCommonStruct _duplicate = new TestCommonStruct();
            _duplicate.foo = foo;
            _duplicate.bar = bar;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TestCommonStruct("
                + "foo=" + foo
                + ", bar=" + bar
                + ")";
        }
        
        public int foo() {
            return this.foo;
        }
        
        public int bar() {
            return this.bar;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TestCommonStruct setFoo(int v) {
            this.foo = v;
            return this;
        }
        
        public TestCommonStruct setBar(int v) {
            this.bar = v;
            return this;
        }
    }
}
