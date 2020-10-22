/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
// Avoid shadowing mutable `file` in AbstractIndex
// 定义位移索引 保存<相对位移值, 保存该消息的日志段文件中该消息第一个字节物理文件位置>
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
  extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {

  import OffsetIndex._

  /**
   * 相对位移值用4字节表示(虽然使用了Long类型 但是该位移值是相对于起始位移值baseOffset而言的 -> [真实位移值 - 起始位移值] 所以只占用4字节 节省了磁盘空间) 物理磁盘位置也用4字节表示
   * broker端参数log.segment.bytes是整型 即Kafka每个日志段文件的大小不会超过 2 `^` 32(4GB)
   * 说明同一个日志段文件上的位移值 - baseOffset的差值一定在整数范围内 所以保存4字节就足够了
   *
   * @return
   */
  override def entrySize = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${mmap.position()}")

  /**
   * The last entry in the index
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
   * Find the largest offset less than or equal to the given targetOffset
   * and return a pair holding this offset and its corresponding physical file position.
   *
   * 定位消息所在的物理磁盘位置
   *
   * @param targetOffset The offset to look up. 目标位移值参数
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate // 复制mmap 共享索引文件的内容
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY) // 内部调用二分查找找到索引项对象所在的槽
      if (slot == -1)
        OffsetPosition(baseOffset, 0) // 没有找到返回一个空的槽 即物理文件从头开始读
      else
        parseEntry(idx, slot) // 返回slot槽对应的索引项  即不大于给定位移值 targetOffset 的最大位移值
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot))
    }
  }

  /**
   * 计算相对位移值
   *
   * @param buffer
   * @param n
   * @return
   */
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  /**
   * 计算物理位移值
   *
   * @param buffer
   * @param n
   * @return
   */
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  /**
   * {@link IndexEntry} 的实现
   * baseOffset + relativeOffset(buffer, n) 用于还原完整位移值
   * physical(buffer, n)计算出消息在日志度文件中的物理位置
   * 将绝对位移值和物理位移值存入
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n      the slot.
   *               查找给定ByteBuffer中保存的第n个索引项(第n个槽)
   * @return the index entry stored in the given slot.
   */
  override protected def parseEntry(buffer: ByteBuffer, n: Int): OffsetPosition = {
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   *
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from index ${file.getAbsolutePath}, " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   *
   * 向索引文件中写入新的索引项
   *
   * @throws InvalidOffsetException if the offset causes index offset to overflow
   */
  def append(offset: Long, position: Int): Unit = {
    inLock(lock) { // 索引对象的各个属性可能被多个线程并发修改 需要加锁保证线程安全
      // 1.判断日志文件是否已经写满
      // 使用Scala的require方法抛出的illegalArgumentException 但是该问题不是由于传入参数导致的 二是由于索引文件处于不恰当的状态 即抛出illegalStateException更加合适
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      // 2.Kafka不允许写入一个比最新索引项还小的索引项 所以必须满足以下两个条件才允许写入:
      // ----a.当前索引文件为空
      // ----b.待写入的索引项位移值 > 当前以写入的索引项位移值(Kafka规定索引项中的位移值必须是单调增加的)
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"Adding index entry $offset => $position to ${file.getAbsolutePath}")
        mmap.putInt(relativeOffset(offset)) // 3A.向mmap写入相对位移值
        mmap.putInt(position) // 3B.向mmap写入物理位移值
        // 4. 更新元数据信息 即索引项计数和当前索引项最新位移
        _entries += 1
        _lastOffset = offset
        // 5.检验写入的的索引项格式是否满足要求 即索引项个数 * 单个索引项占用字节数 == 当前文件物理大小 不想等说明文件已损坏
        require(_entries * entrySize == mmap.position(), s"$entries entries but file position in index is ${mmap.position()}.")
      } else {
        // 无法写入时抛出的异常
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than" +
          s" the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if (slot < 0)
          0 // 截断全部
        else if (relativeOffset(idx, slot) == offset - baseOffset)
          slot // 截断该消息及之后的内容
        else
          slot + 1 // 截断比下一个消息的位移大的所有消息
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   *
   * @param entries 表示截取到哪个槽
   */
  private def truncateToEntries(entries: Int): Unit = {
    inLock(lock) {
      _entries = entries // 更新索引项计数
      mmap.position(_entries * entrySize) // 表示要截断到位置
      _lastOffset = lastEntry.offset // 更新索引文件的最大位移值
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries;" +
        s" position is now ${mmap.position()} and last offset is now ${_lastOffset}")
    }
  }

  override def sanityCheck(): Unit = {
    if (_entries != 0 && _lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is less than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}

object OffsetIndex extends Logging {
  override val loggerName: String = classOf[OffsetIndex].getName
}
