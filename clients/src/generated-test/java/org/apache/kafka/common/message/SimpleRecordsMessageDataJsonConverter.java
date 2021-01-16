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
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.MemoryRecords;

import static org.apache.kafka.common.message.SimpleRecordsMessageData.*;

public class SimpleRecordsMessageDataJsonConverter {
    public static SimpleRecordsMessageData read(JsonNode _node, short _version) {
        SimpleRecordsMessageData _object = new SimpleRecordsMessageData();
        JsonNode _topicNode = _node.get("topic");
        if (_topicNode == null) {
            throw new RuntimeException("SimpleRecordsMessageData: unable to locate field 'topic', which is mandatory in version " + _version);
        } else {
            if (!_topicNode.isTextual()) {
                throw new RuntimeException("SimpleRecordsMessageData expected a string type, but got " + _node.getNodeType());
            }
            _object.topic = _topicNode.asText();
        }
        JsonNode _recordSetNode = _node.get("recordSet");
        if (_recordSetNode == null) {
            throw new RuntimeException("SimpleRecordsMessageData: unable to locate field 'recordSet', which is mandatory in version " + _version);
        } else {
            if (_recordSetNode.isNull()) {
                _object.recordSet = null;
            } else {
                _object.recordSet = MemoryRecords.readableRecords(ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(_recordSetNode, "SimpleRecordsMessageData")));
            }
        }
        return _object;
    }
    public static JsonNode write(SimpleRecordsMessageData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("topic", new TextNode(_object.topic));
        if (_object.recordSet == null) {
            _node.set("recordSet", NullNode.instance);
        } else {
            if (_serializeRecords) {
                _node.set("recordSet", new BinaryNode(new byte[]{}));
            } else {
                _node.set("recordSetSizeInBytes", new IntNode(_object.recordSet.sizeInBytes()));
            }
        }
        return _node;
    }
    public static JsonNode write(SimpleRecordsMessageData _object, short _version) {
        return write(_object, _version, true);
    }
}
