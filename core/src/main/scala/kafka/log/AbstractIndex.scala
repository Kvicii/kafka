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

import java.io.{Closeable, File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{ByteBufferUnmapper, OperatingSystem, Utils}

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * 关于索引的顶层抽象类 封装了所有索引类型的公共操作
 * 由于是抽象基类 Kafka的所有子类自动继承了这4个参数
 *
 * @param _file        The index file.  索引文件 var类型说明可被修改
 *                     自1.1.0版本之后 Kafka允许迁移底层的日志路径 所以索引文件自然要是可以更换的
 * @param baseOffset   the base offset of the segment that this index is corresponding to.
 *                     起始位移值 索引对象对应日志段对象的起始位移值 日志文件和索引文件都是成组出现的
 * @param maxIndexSize The maximum index size in bytes.
 *                     索引文件最大字节数 控制了索引文件的最大长度 传入该参数的值是broker端参数segment.index.bytes值(10M) 即默认情况下Kafka的所有索引文件默认大小都是10MB
 * @param writable
 *                     索引文件的打开方式 True代表以读写方式打开 False代表以只读方式打开
 */
abstract class AbstractIndex(@volatile private var _file: File, val baseOffset: Long,
                             val maxIndexSize: Int = -1, val writable: Boolean) extends Closeable {

  import AbstractIndex._

  // Length of the index file
  @volatile
  private var _length: Long = _

  /**
   * 表示不同索引项的大小
   *
   * @return
   */
  protected def entrySize: Int

  /*
   Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
   avoids blocked disk I/O in most cases.

   To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
   cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
   followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
   well with Kafka's index access pattern.

   However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
   page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
   cached in the page cache).

   For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
   algorithm will read index entries in page #0, 6, 9, 11, and 12.
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
   steps:       |1| | | | | |3| | |4|  |5 |2/6|
   In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
   index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
   operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
   are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
   in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
   and 13:
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
   steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|
   Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
   the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
   and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
   at-least-once produce latency to jump to about 1 second from a few ms.

   Here, we use a more cache-friendly lookup algorithm:
   if (target > indexEntry[end - N]) // if the target is in the last N entries of the index
      binarySearch(end - N, end)
   else
      binarySearch(begin, end - N)

   If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
   lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
   relatively small section, the pages containing this section are more likely to be in the page cache.

   We set N (_warmEntries) to 8192, because
   1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
      lookup. So that, the entire warm section is really "warm".
      When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
      and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
      touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
      SPARC, Power, ARM etc.).
   2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
      settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.

   We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
   section pages are really warm (touched in every lookup) on a typical 4KB-page host.

   In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
   1) support larger warm section
   2) make sure the warm section of low QPS topic-partitions are really warm.
 */
  protected def _warmEntries: Int = 8192 / entrySize

  protected val lock = new ReentrantLock

  /**
   * Kafka索引底层的实现原理 内存映射文件(MappedByteBuffer)
   * 拥有很高的IO性能 文件直接映射到一段虚拟内存 访问内存映射文件的速度要快于普通文件的读写速度
   * 在Linux操作系统 这段映射的内存区域直接就是操作系统的Page Cache 意味着不需要将数据拷贝到用户态空间 避免了时间/空间消耗
   *
   * 创建MappedByteBuffer对象实例 {@link AbstractIndex} 的其他大部分操作都和mmap有关
   */
  @volatile
  protected var mmap: MappedByteBuffer = {
    // 1.尝试创建索引对象对应的物理磁盘文件
    val newlyCreated = file.createNewFile()
    // 2.根据writable参数以指定方式(读写/只读)打开索引文件
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary */
      if (newlyCreated) {
        if (maxIndexSize < entrySize) // 预设的索引文件不能太小 如果连一个索引项都保存不了直接抛出异常
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        // 3.设置索引文件长度 roundDownToExactMultiple计算的是不超过maxIndexSize的最大整数倍entrySize
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize)) // 比如maxIndexSize=67 entrySize=8 那么调整后的文件长度为64
      }
      // info.索引对象的文件长度是指索引对象底层物理文件的大小 索引对象的长度字段是内存中索引类实例的长度属性
      /* memory-map the file */
      _length = raf.length() // 4.更新索引对象长度字段_length
      // 5.更具writable参数创建只读/读写MappedByteBuffer
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      // 6.索引文件是全新创建的 将MappedByteBuffer对象的当前位置设置为0
      if (newlyCreated)
        idx.position(0)
      else
      // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize)) // 如果索引文件已存在 将MappedByteBuffer对象的当前位置调整为最后一个索引项所在的位置
      idx //7.返回创建的MappedByteBuffer对象
    } finally {
      CoreUtils.swallow(raf.close(), AbstractIndex) // 关闭打开索引文件句柄
    }
  }

  /**
   * The maximum number of entries this index can hold
   * 索引对象最多能容纳多少个索引项
   */
  @volatile
  private[this] var _maxEntries: Int = mmap.limit() / entrySize

  /** The number of entries in this index */
  // 计算索引对象中当前有多少个索引项
  @volatile
  protected var _entries: Int = mmap.position() / entrySize

  /**
   * True iff there are no more slots available in this index
   * 判断当前索引文件是否已经写满
   */
  def isFull: Boolean = _entries >= _maxEntries

  def file: File = _file

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *
   * @param newSize new size of the index file
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        debug(s"Index ${file.getAbsolutePath} was not resized because it already has size $roundedNewSize")
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
          if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          debug(s"Resized ${file.getAbsolutePath} to $roundedNewSize, position is ${mmap.position()} " +
            s"and limit is ${mmap.limit()}")
          true
        } finally {
          CoreUtils.swallow(raf.close(), AbstractIndex)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File): Unit = {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally _file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush(): Unit = {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    closeHandler()
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize(): Unit = {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes: Int = entrySize * _entries

  /** Close the index */
  def close(): Unit = {
    trimToValidSize()
    closeHandler()
  }

  def closeHandler(): Unit = {
    // On JVM, a memory mapping is typically unmapped by garbage collector.
    // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
    // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
    // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
   *
   * 将Long型位移值转换为相对位移值
   *
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
   *
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    if (mmap != null) {
      try forceUnmap()
      catch {
        case t: Throwable => error(s"Error unmapping index $file", t)
      }
    }
  }

  /**
   * Forcefully free the buffer's mmap.
   */
  protected[log] def forceUnmap(): Unit = {
    try ByteBufferUnmapper.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows or z/OS. We do this
   * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   * 查找给定的索引项 即将相对位移值还原为之前的完整位移值
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n      the slot. 查找给定ByteBuffer中保存的第n个索引项(第n个槽)
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx    The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.
   * 二分查找算法找出索引项对象的所在的第n个槽
   *
   * 之前版本问题:
   * 由于Kafka使用了Linux的Page Cache完成MappedByteBuffer的映射 而操作系统的Page Cache大都使用了LRU或近似LRU的机制来管理
   * Kafka是在文件末尾追加写入的 索引的查询几乎也是发生在索引文件尾部 这就导致了在查询时 可能目前的Page没有写满 二分查找经过的Page一直是其中几个
   * 当目前Page写满重新申请Page时 之前访问的部分Page可能会失效 导致缺页中断(Page Fault) 此时Kafka线程会被阻塞 等待从磁盘中读数据到缓存页
   *
   * 每当索引文件占用 Page 数发生变化时 就会强行变更二分查找的搜索路径 从而出现不在页缓存的冷数据必须要加载到页缓存的情形 而这种加载过程是非常耗时的
   *
   * 解决方案:
   * 将所有索引项分成两个部分:热区(Warm Area)和冷区(Cold Area) 然后分别在这两个区域内执行二分查找算法
   * 这个改进版算法的最大好处在于 查询最热那部分数据所遍历的 Page 永远是固定的 因此大概率在页缓存中 从而避免无意义的 Page Fault
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): (Int, Int) = {
    // check if the index is empty
    // 如果当前索引对象没有索引项直接返回<-1, -1>
    if (_entries == 0)
      return (-1, -1)

    // 封装原版的二分查找算法
    def binarySearch(begin: Int, end: Int): (Int, Int) = {
      // binary search for the entry
      var lo = begin
      var hi = end
      while (lo < hi) {
        val mid = (lo + hi + 1) >>> 1
        val found = parseEntry(idx, mid)
        val compareResult = compareIndexEntry(found, target, searchEntity)
        if (compareResult > 0)
          hi = mid - 1
        else if (compareResult < 0)
          lo = mid
        else
          return (mid, mid)
      }
      (lo, if (lo == _entries - 1) -1 else lo + 1)
    }

    // 确定热区首个索引项位于哪个槽 _warmEntries就是所谓的分割线 目前固定为8192字节处
    // 如果是 {@link OffsetIndex}  _warmEntries = 8192 / entrySize 即第1024个槽 其中entrySize值为8 也就是OffsetIndex中定义的值
    // 如果是 {@link TimeIndex} _warmEntries = 8192 / entrySize = 682 即第682个槽 其中entrySize值为12 也就是TimeIndex中定义的值
    val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries) // 热区位于尾部
    // check if the target offset is in the warm section of the index
    // 判断查找位移值target是在热区还是冷区
    if (compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
      // 热区直接搜索
      return binarySearch(firstHotEntry, _entries - 1)
    }

    // check if the target offset is smaller than the least offset
    // 确保target位移值不能 < 当前最小位移值
    if (compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)
    // 如果在冷区搜索冷区
    binarySearch(0, firstHotEntry)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchType): Int = {
    searchEntity match {
      case IndexSearchType.KEY => java.lang.Long.compare(indexEntry.indexKey, target)
      case IndexSearchType.VALUE => java.lang.Long.compare(indexEntry.indexValue, target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset // 计算位移值与起始位移值的差值 即相对位移值
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt) // 校验通过返回这个值
  }
}

object AbstractIndex extends Logging {
  override val loggerName: String = classOf[AbstractIndex].getName
}

sealed trait IndexSearchType

object IndexSearchType {

  case object KEY extends IndexSearchType

  case object VALUE extends IndexSearchType

}
