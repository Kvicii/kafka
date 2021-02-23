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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;

import static org.apache.kafka.common.message.SimpleExampleMessageData.*;

public class SimpleExampleMessageDataJsonConverter {
    public static SimpleExampleMessageData read(JsonNode _node, short _version) {
        SimpleExampleMessageData _object = new SimpleExampleMessageData();
        JsonNode _processIdNode = _node.get("processId");
        if (_processIdNode == null) {
            if (_version >= 1) {
                throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'processId', which is mandatory in version " + _version);
            } else {
                _object.processId = Uuid.ZERO_UUID;
            }
        } else {
            if (!_processIdNode.isTextual()) {
                throw new RuntimeException("SimpleExampleMessageData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.processId = Uuid.fromString(_processIdNode.asText());
        }
        JsonNode _myTaggedIntArrayNode = _node.get("myTaggedIntArray");
        if (_myTaggedIntArrayNode == null) {
            _object.myTaggedIntArray = new ArrayList<Integer>(0);
        } else {
            if (!_myTaggedIntArrayNode.isArray()) {
                throw new RuntimeException("SimpleExampleMessageData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Integer> _collection = new ArrayList<Integer>(_myTaggedIntArrayNode.size());
            _object.myTaggedIntArray = _collection;
            for (JsonNode _element : _myTaggedIntArrayNode) {
                _collection.add(MessageUtil.jsonNodeToInt(_element, "SimpleExampleMessageData element"));
            }
        }
        JsonNode _myNullableStringNode = _node.get("myNullableString");
        if (_myNullableStringNode == null) {
            _object.myNullableString = null;
        } else {
            if (_myNullableStringNode.isNull()) {
                _object.myNullableString = null;
            } else {
                if (!_myNullableStringNode.isTextual()) {
                    throw new RuntimeException("SimpleExampleMessageData expected a string type, but got " + _node.getNodeType());
                }
                _object.myNullableString = _myNullableStringNode.asText();
            }
        }
        JsonNode _myInt16Node = _node.get("myInt16");
        if (_myInt16Node == null) {
            _object.myInt16 = (short) 123;
        } else {
            _object.myInt16 = MessageUtil.jsonNodeToShort(_myInt16Node, "SimpleExampleMessageData");
        }
        JsonNode _myFloat64Node = _node.get("myFloat64");
        if (_myFloat64Node == null) {
            _object.myFloat64 = Double.parseDouble("12.34");
        } else {
            _object.myFloat64 = MessageUtil.jsonNodeToDouble(_myFloat64Node, "SimpleExampleMessageData");
        }
        JsonNode _myStringNode = _node.get("myString");
        if (_myStringNode == null) {
            _object.myString = "";
        } else {
            if (!_myStringNode.isTextual()) {
                throw new RuntimeException("SimpleExampleMessageData expected a string type, but got " + _node.getNodeType());
            }
            _object.myString = _myStringNode.asText();
        }
        JsonNode _myBytesNode = _node.get("myBytes");
        if (_myBytesNode == null) {
            _object.myBytes = Bytes.EMPTY;
        } else {
            if (_myBytesNode.isNull()) {
                _object.myBytes = null;
            } else {
                _object.myBytes = MessageUtil.jsonNodeToBinary(_myBytesNode, "SimpleExampleMessageData");
            }
        }
        JsonNode _taggedUuidNode = _node.get("taggedUuid");
        if (_taggedUuidNode == null) {
            _object.taggedUuid = Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A");
        } else {
            if (!_taggedUuidNode.isTextual()) {
                throw new RuntimeException("SimpleExampleMessageData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.taggedUuid = Uuid.fromString(_taggedUuidNode.asText());
        }
        JsonNode _taggedLongNode = _node.get("taggedLong");
        if (_taggedLongNode == null) {
            _object.taggedLong = 0xcafcacafcacafcaL;
        } else {
            _object.taggedLong = MessageUtil.jsonNodeToLong(_taggedLongNode, "SimpleExampleMessageData");
        }
        JsonNode _zeroCopyByteBufferNode = _node.get("zeroCopyByteBuffer");
        if (_zeroCopyByteBufferNode == null) {
            if ((_version >= 1) && (_version <= 1)) {
                throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'zeroCopyByteBuffer', which is mandatory in version " + _version);
            } else {
                _object.zeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
            }
        } else {
            _object.zeroCopyByteBuffer = ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(_zeroCopyByteBufferNode, "SimpleExampleMessageData"));
        }
        JsonNode _nullableZeroCopyByteBufferNode = _node.get("nullableZeroCopyByteBuffer");
        if (_nullableZeroCopyByteBufferNode == null) {
            if ((_version >= 1) && (_version <= 1)) {
                throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'nullableZeroCopyByteBuffer', which is mandatory in version " + _version);
            } else {
                _object.nullableZeroCopyByteBuffer = ByteUtils.EMPTY_BUF;
            }
        } else {
            if (_nullableZeroCopyByteBufferNode.isNull()) {
                _object.nullableZeroCopyByteBuffer = null;
            } else {
                _object.nullableZeroCopyByteBuffer = ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(_nullableZeroCopyByteBufferNode, "SimpleExampleMessageData"));
            }
        }
        JsonNode _myStructNode = _node.get("myStruct");
        if (_myStructNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'myStruct', which is mandatory in version " + _version);
            } else {
                _object.myStruct = new MyStruct();
            }
        } else {
            _object.myStruct = MyStructJsonConverter.read(_myStructNode, _version);
        }
        JsonNode _myTaggedStructNode = _node.get("myTaggedStruct");
        if (_myTaggedStructNode == null) {
            _object.myTaggedStruct = new TaggedStruct();
        } else {
            _object.myTaggedStruct = TaggedStructJsonConverter.read(_myTaggedStructNode, _version);
        }
        JsonNode _myCommonStructNode = _node.get("myCommonStruct");
        if (_myCommonStructNode == null) {
            throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'myCommonStruct', which is mandatory in version " + _version);
        } else {
            _object.myCommonStruct = TestCommonStructJsonConverter.read(_myCommonStructNode, _version);
        }
        JsonNode _myOtherCommonStructNode = _node.get("myOtherCommonStruct");
        if (_myOtherCommonStructNode == null) {
            throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'myOtherCommonStruct', which is mandatory in version " + _version);
        } else {
            _object.myOtherCommonStruct = TestCommonStructJsonConverter.read(_myOtherCommonStructNode, _version);
        }
        JsonNode _myUint16Node = _node.get("myUint16");
        if (_myUint16Node == null) {
            if (_version >= 1) {
                throw new RuntimeException("SimpleExampleMessageData: unable to locate field 'myUint16', which is mandatory in version " + _version);
            } else {
                _object.myUint16 = 33000;
            }
        } else {
            _object.myUint16 = MessageUtil.jsonNodeToUnsignedShort(_myUint16Node, "SimpleExampleMessageData");
        }
        return _object;
    }
    public static JsonNode write(SimpleExampleMessageData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_version >= 1) {
            _node.set("processId", new TextNode(_object.processId.toString()));
        } else {
            if (!_object.processId.equals(Uuid.ZERO_UUID)) {
                throw new UnsupportedVersionException("Attempted to write a non-default processId at version " + _version);
            }
        }
        if (_version >= 1) {
            if (!_object.myTaggedIntArray.isEmpty()) {
                ArrayNode _myTaggedIntArrayArray = new ArrayNode(JsonNodeFactory.instance);
                for (Integer _element : _object.myTaggedIntArray) {
                    _myTaggedIntArrayArray.add(new IntNode(_element));
                }
                _node.set("myTaggedIntArray", _myTaggedIntArrayArray);
            }
        } else {
            if (!_object.myTaggedIntArray.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default myTaggedIntArray at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.myNullableString != null) {
                _node.set("myNullableString", new TextNode(_object.myNullableString));
            }
        } else {
            if (_object.myNullableString != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default myNullableString at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.myInt16 != (short) 123) {
                _node.set("myInt16", new ShortNode(_object.myInt16));
            }
        } else {
            if (_object.myInt16 != (short) 123) {
                throw new UnsupportedVersionException("Attempted to write a non-default myInt16 at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.myFloat64 != Double.parseDouble("12.34")) {
                _node.set("myFloat64", new DoubleNode(_object.myFloat64));
            }
        } else {
            if (_object.myFloat64 != Double.parseDouble("12.34")) {
                throw new UnsupportedVersionException("Attempted to write a non-default myFloat64 at version " + _version);
            }
        }
        if (_version >= 1) {
            if (!_object.myString.equals("")) {
                _node.set("myString", new TextNode(_object.myString));
            }
        } else {
            if (!_object.myString.equals("")) {
                throw new UnsupportedVersionException("Attempted to write a non-default myString at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.myBytes == null || _object.myBytes.length != 0) {
                if (_object.myBytes == null) {
                    _node.set("myBytes", NullNode.instance);
                } else {
                    _node.set("myBytes", new BinaryNode(Arrays.copyOf(_object.myBytes, _object.myBytes.length)));
                }
            }
        } else {
            if (_object.myBytes == null || _object.myBytes.length != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default myBytes at version " + _version);
            }
        }
        if (_version >= 1) {
            if (!_object.taggedUuid.equals(Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"))) {
                _node.set("taggedUuid", new TextNode(_object.taggedUuid.toString()));
            }
        } else {
            if (!_object.taggedUuid.equals(Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"))) {
                throw new UnsupportedVersionException("Attempted to write a non-default taggedUuid at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.taggedLong != 0xcafcacafcacafcaL) {
                _node.set("taggedLong", new LongNode(_object.taggedLong));
            }
        } else {
            if (_object.taggedLong != 0xcafcacafcacafcaL) {
                throw new UnsupportedVersionException("Attempted to write a non-default taggedLong at version " + _version);
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            _node.set("zeroCopyByteBuffer", new BinaryNode(MessageUtil.byteBufferToArray(_object.zeroCopyByteBuffer)));
        } else {
            if (_object.zeroCopyByteBuffer.hasRemaining()) {
                throw new UnsupportedVersionException("Attempted to write a non-default zeroCopyByteBuffer at version " + _version);
            }
        }
        if ((_version >= 1) && (_version <= 1)) {
            if (_object.nullableZeroCopyByteBuffer == null) {
                _node.set("nullableZeroCopyByteBuffer", NullNode.instance);
            } else {
                _node.set("nullableZeroCopyByteBuffer", new BinaryNode(MessageUtil.byteBufferToArray(_object.nullableZeroCopyByteBuffer)));
            }
        } else {
            if (_object.nullableZeroCopyByteBuffer == null || _object.nullableZeroCopyByteBuffer.remaining() > 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default nullableZeroCopyByteBuffer at version " + _version);
            }
        }
        if (_version >= 2) {
            _node.set("myStruct", MyStructJsonConverter.write(_object.myStruct, _version, _serializeRecords));
        } else {
            if (!_object.myStruct.equals(new MyStruct())) {
                throw new UnsupportedVersionException("Attempted to write a non-default myStruct at version " + _version);
            }
        }
        if (_version >= 2) {
            if (!_object.myTaggedStruct.equals(new TaggedStruct())) {
                _node.set("myTaggedStruct", TaggedStructJsonConverter.write(_object.myTaggedStruct, _version, _serializeRecords));
            }
        } else {
            if (!_object.myTaggedStruct.equals(new TaggedStruct())) {
                throw new UnsupportedVersionException("Attempted to write a non-default myTaggedStruct at version " + _version);
            }
        }
        _node.set("myCommonStruct", TestCommonStructJsonConverter.write(_object.myCommonStruct, _version, _serializeRecords));
        _node.set("myOtherCommonStruct", TestCommonStructJsonConverter.write(_object.myOtherCommonStruct, _version, _serializeRecords));
        if (_version >= 1) {
            _node.set("myUint16", new IntNode(_object.myUint16));
        } else {
            if (_object.myUint16 != 33000) {
                throw new UnsupportedVersionException("Attempted to write a non-default myUint16 at version " + _version);
            }
        }
        return _node;
    }
    public static JsonNode write(SimpleExampleMessageData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class MyStructJsonConverter {
        public static MyStruct read(JsonNode _node, short _version) {
            MyStruct _object = new MyStruct();
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MyStruct");
            }
            JsonNode _structIdNode = _node.get("structId");
            if (_structIdNode == null) {
                throw new RuntimeException("MyStruct: unable to locate field 'structId', which is mandatory in version " + _version);
            } else {
                _object.structId = MessageUtil.jsonNodeToInt(_structIdNode, "MyStruct");
            }
            JsonNode _arrayInStructNode = _node.get("arrayInStruct");
            if (_arrayInStructNode == null) {
                throw new RuntimeException("MyStruct: unable to locate field 'arrayInStruct', which is mandatory in version " + _version);
            } else {
                if (!_arrayInStructNode.isArray()) {
                    throw new RuntimeException("MyStruct expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<StructArray> _collection = new ArrayList<StructArray>(_arrayInStructNode.size());
                _object.arrayInStruct = _collection;
                for (JsonNode _element : _arrayInStructNode) {
                    _collection.add(StructArrayJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(MyStruct _object, short _version, boolean _serializeRecords) {
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MyStruct");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("structId", new IntNode(_object.structId));
            ArrayNode _arrayInStructArray = new ArrayNode(JsonNodeFactory.instance);
            for (StructArray _element : _object.arrayInStruct) {
                _arrayInStructArray.add(StructArrayJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("arrayInStruct", _arrayInStructArray);
            return _node;
        }
        public static JsonNode write(MyStruct _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class StructArrayJsonConverter {
        public static StructArray read(JsonNode _node, short _version) {
            StructArray _object = new StructArray();
            JsonNode _arrayFieldIdNode = _node.get("arrayFieldId");
            if (_arrayFieldIdNode == null) {
                throw new RuntimeException("StructArray: unable to locate field 'arrayFieldId', which is mandatory in version " + _version);
            } else {
                _object.arrayFieldId = MessageUtil.jsonNodeToInt(_arrayFieldIdNode, "StructArray");
            }
            return _object;
        }
        public static JsonNode write(StructArray _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("arrayFieldId", new IntNode(_object.arrayFieldId));
            return _node;
        }
        public static JsonNode write(StructArray _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TaggedStructJsonConverter {
        public static TaggedStruct read(JsonNode _node, short _version) {
            TaggedStruct _object = new TaggedStruct();
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TaggedStruct");
            }
            JsonNode _structIdNode = _node.get("structId");
            if (_structIdNode == null) {
                throw new RuntimeException("TaggedStruct: unable to locate field 'structId', which is mandatory in version " + _version);
            } else {
                if (!_structIdNode.isTextual()) {
                    throw new RuntimeException("TaggedStruct expected a string type, but got " + _node.getNodeType());
                }
                _object.structId = _structIdNode.asText();
            }
            return _object;
        }
        public static JsonNode write(TaggedStruct _object, short _version, boolean _serializeRecords) {
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of TaggedStruct");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("structId", new TextNode(_object.structId));
            return _node;
        }
        public static JsonNode write(TaggedStruct _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TestCommonStructJsonConverter {
        public static TestCommonStruct read(JsonNode _node, short _version) {
            TestCommonStruct _object = new TestCommonStruct();
            JsonNode _fooNode = _node.get("foo");
            if (_fooNode == null) {
                throw new RuntimeException("TestCommonStruct: unable to locate field 'foo', which is mandatory in version " + _version);
            } else {
                _object.foo = MessageUtil.jsonNodeToInt(_fooNode, "TestCommonStruct");
            }
            JsonNode _barNode = _node.get("bar");
            if (_barNode == null) {
                throw new RuntimeException("TestCommonStruct: unable to locate field 'bar', which is mandatory in version " + _version);
            } else {
                _object.bar = MessageUtil.jsonNodeToInt(_barNode, "TestCommonStruct");
            }
            return _object;
        }
        public static JsonNode write(TestCommonStruct _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("foo", new IntNode(_object.foo));
            _node.set("bar", new IntNode(_object.bar));
            return _node;
        }
        public static JsonNode write(TestCommonStruct _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
