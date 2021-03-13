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
package org.apache.kafka.common.network;

import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private final MemoryPool memoryPool;
    private int requestedBufferSize = -1;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    /**
     * 判断是否读完消息
     *
     * @return
     */
    @Override
    public boolean complete() {
        // 存储分隔符的ByteBuffer读满了 && 存储实际数据的ByteBuffer读满了 说明读完了一条消息返回true
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    /**
     * 处理读事件的方法
     * READABLE事件可能会出现拆包问题和粘包问题 具体解决都是在该方法中实现
     * <p>
     * Kafka解决READABLE事件的粘包问题 --> 每个响应结果中间必须插入一个特殊的几个字节的分隔符 一般来说比较经典的就是使用代表响应消息大小的4字节的Integer类型数字插入到响应消息前面
     * <p>
     * 假如size是4个字节 一次read就读取到了2个字节 连size都没有读取完毕出现了拆包 或者读取到了一个size 199个字节 但是在读取响应消息的时候 就读取到了162个字节
     * 即响应消息没有读取完毕
     * Kafka解决READABLE事件的拆包问题 -->
     * 等待下次轮询时进行处理
     *
     * @param channel The channel to read from
     * @return 本次读取的字节数
     * @throws IOException
     */
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        // 本次读取的字节数
        int read = 0;
        // --------------------------------读取数据头(数据长度分隔符)START--------------------------------
        // 返回limit和position之间是否有剩余空间 Integer作为分隔符 limit和position之间的空间是4
        if (size.hasRemaining()) {  // 如果发生拆包 此处连4个字节可能都无法读取完(如position = 2 limit = 4) 此时会等到下次轮询再继续读取
            // 从Channel中读取4字节的数据长度数字(Integer) 写入到ByteBuffer
            int bytesRead = channel.read(size);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            read += bytesRead;
            if (!size.hasRemaining()) { // 如果已经读取到了4字节的长度信息 position = 4 limit = 4 代表ByteBuffer读满了
                // 把position设置为0
                // 常见于一个ByteBuffer写满之后 将position重置为0 此时就可以从ByteBuffer中读取数据了
                size.rewind();
                // 从position = 0的位置开始读4个字节的数据信息长度
                int receiveSize = size.getInt();
                if (receiveSize < 0) {
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                }
                if (maxSize != UNLIMITED && receiveSize > maxSize) {
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                }
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }
        // --------------------------------读取数据头(数据长度分隔符)END--------------------------------
        // --------------------------------读取数据包START--------------------------------
        if (buffer == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            // 分配一个数据长度大小的ByteBuffer
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            if (buffer == null) {
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
            }
        }
        if (buffer != null) {
            // 读取响应数据到ByteBuffer
            // 此处也可能会发生拆包问题 和数据头的处理方式一致 都会等到下次轮询继续读取
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            read += bytesRead;
        }
        // --------------------------------读取数据包END--------------------------------

        return read;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer);
            buffer = null;
        }
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

    public int bytesRead() {
        if (buffer == null) {
            return size.position();
        }
        return buffer.position() + size.position();
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     */
    public int size() {
        return payload().limit() + size.limit();
    }

}
