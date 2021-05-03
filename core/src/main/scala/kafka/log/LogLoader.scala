/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}

import kafka.common.LogSegmentOffsetOverflowException
import kafka.log.Log.{CleanedFileSuffix, DeletedFileSuffix, SwapFileSuffix, isIndexFile, isLogFile, offsetFromFile, offsetFromFileName}
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, Set, mutable}

case class LoadedLogOffsets(logStartOffset: Long,
                            recoveryPoint: Long,
                            nextOffsetMetadata: LogOffsetMetadata)

/**
 * @param dir The directory from which log segments need to be loaded
 * @param topicPartition The topic partition associated with the log being loaded
 * @param config The configuration settings for the log being loaded
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle log
 *                             directory failure
 * @param hadCleanShutdown Boolean flag to indicate whether the associated log previously had a
 *                         clean shutdown
 * @param segments The LogSegments instance into which segments recovered from disk will be
 *                 populated
 * @param logStartOffsetCheckpoint The checkpoint of the log start offset
 * @param recoveryPointCheckpoint The checkpoint of the offset at which to begin the recovery
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is
 *                                  considered expired
 * @param leaderEpochCache An optional LeaderEpochFileCache instance to be updated during recovery
 * @param producerStateManager The ProducerStateManager instance to be updated during recovery
 */
case class LoadLogParams(dir: File,
                         topicPartition: TopicPartition,
                         config: LogConfig,
                         scheduler: Scheduler,
                         time: Time,
                         logDirFailureChannel: LogDirFailureChannel,
                         hadCleanShutdown: Boolean,
                         segments: LogSegments,
                         logStartOffsetCheckpoint: Long,
                         recoveryPointCheckpoint: Long,
                         maxProducerIdExpirationMs: Int,
                         leaderEpochCache: Option[LeaderEpochFileCache],
                         producerStateManager: ProducerStateManager) {
  val logIdentifier: String = s"[LogLoader partition=$topicPartition, dir=${dir.getParent}]"
}

/**
 * This object is responsible for all activities related with recovery of log segments from disk.
 */
object LogLoader extends Logging {
  /**
   * Load the log segments from the log files on disk, and return the components of the loaded log.
   * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
   * to reflect the contents of the loaded log.
   *
   * In the context of the calling thread, this function does not need to convert IOException to
   * KafkaStorageException because it is only called before all logs are loaded.
   *
   * @param params The parameters for the log being loaded from disk
   *
   * @return the offsets of the Log successfully loaded from disk
   *
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
   *                                           overflow index offset
   */
  def load(params: LoadLogParams): LoadedLogOffsets = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    val swapFiles = removeTempFilesAndCollectSwapFiles(params)

    // Now do a second pass and load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    retryOnOffsetOverflow(params, {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      params.segments.close()
      params.segments.clear()
      loadSegmentFiles(params)
    })

    completeSwapOperations(swapFiles, params)

    val (newRecoveryPoint: Long, nextOffset: Long) = {
      if (!params.dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
        val (newRecoveryPoint, nextOffset) = retryOnOffsetOverflow(params, {
          recoverLog(params)
        })

        // reset the index size of the currently active log segment to allow more entries
        params.segments.lastSegment.get.resizeIndexes(params.config.maxIndexSize)
        (newRecoveryPoint, nextOffset)
      } else {
        if (params.segments.isEmpty) {
          params.segments.add(
            LogSegment.open(
              dir = params.dir,
              baseOffset = 0,
              params.config,
              time = params.time,
              initFileSize = params.config.initFileSize))
        }
        (0L, 0L)
      }
    }

    params.leaderEpochCache.foreach(_.truncateFromEnd(nextOffset))
    val newLogStartOffset = math.max(params.logStartOffsetCheckpoint, params.segments.firstSegment.get.baseOffset)
    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    params.leaderEpochCache.foreach(_.truncateFromStart(params.logStartOffsetCheckpoint))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    if (!params.producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")

    // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
    // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
    // deletion.
    params.producerStateManager.removeStraySnapshots(params.segments.baseOffsets.toSeq)
    Log.rebuildProducerState(
      params.producerStateManager,
      params.segments,
      newLogStartOffset,
      nextOffset,
      params.config.messageFormatVersion.recordVersion,
      params.time,
      reloadFromCleanShutdown = params.hadCleanShutdown)

    val activeSegment = params.segments.lastSegment.get
    LoadedLogOffsets(
      newLogStartOffset,
      newRecoveryPoint,
      LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size))
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   * @param params The parameters for the log being loaded from disk
   * @return Set of .swap files that are valid to be swapped in as segment files
   */
  private def removeTempFilesAndCollectSwapFiles(params: LoadLogParams): Set[File] = {

    // 删除日志文件对应的索引文件
    def deleteIndicesIfExist(baseFile: File, suffix: String = ""): Unit = {
      info(s"${params.logIdentifier} Deleting index files with suffix $suffix for baseFile $baseFile")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(params.dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.timeIndexFile(params.dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(params.dir, offset, suffix).toPath)
    }

    val swapFiles = mutable.Set[File]()
    val cleanFiles = mutable.Set[File]()
    var minCleanedFileOffset = Long.MaxValue

    // 遍历分区日志路径下的所有文件
    for (file <- params.dir.listFiles if file.isFile) {
      // 不可读直接抛异常
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix)) {
        // 如果以.deleted结尾说明是上一次Failure遗留下来的文件 直接删除
        debug(s"${params.logIdentifier} Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(CleanedFileSuffix)) {
        // 如果以.cleaned结尾 获取其位移值 并将该文件加入待删除文件集合
        minCleanedFileOffset = Math.min(offsetFromFileName(filename), minCleanedFileOffset)
        cleanFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        // 如果以.swap结尾
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        info(s"${params.logIdentifier} Found file ${file.getAbsolutePath} from interrupted swap operation.")
        if (Log.isIndexFile(baseFile)) {
          // 如果该文件是索引文件 删除
          deleteIndicesIfExist(baseFile)
        } else if (Log.isLogFile(baseFile)) {
          // 如果该文件是日志文件 删除索引文件 并将该文件加入待恢复的swap集合
          deleteIndicesIfExist(baseFile)
          swapFiles += file
        }
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    // 从待恢复的swap集合中找出起始位移值 > minCleanedFileOffset的文件 直接删除这些无效的.swap文件
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"${params.logIdentifier} Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
      deleteIndicesIfExist(baseFile, SwapFileSuffix)
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    // 清除所有待删除文件集合中的文件
    cleanFiles.foreach { file =>
      debug(s"${params.logIdentifier} Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    validSwapFiles
  }

  /**
   * Retries the provided function only whenever an LogSegmentOffsetOverflowException is raised by
   * it during execution. Before every retry, the overflowed segment is split into one or more segments
   * such that there is no offset overflow in any of them.
   *
   * @param params The parameters for the log being loaded from disk
   * @param fn The function to be executed
   * @return The value returned by the function, if successful
   * @throws Exception whenever the executed function throws any exception other than
   *                   LogSegmentOffsetOverflowException, the same exception is raised to the caller
   */
  private def retryOnOffsetOverflow[T](params: LoadLogParams, fn: => T): T = {
    while (true) {
      try {
        return fn
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"${params.logIdentifier} Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          Log.splitOverflowedSegment(
            e.segment,
            params.segments,
            params.dir,
            params.topicPartition,
            params.config,
            params.scheduler,
            params.logDirFailureChannel,
            params.producerStateManager)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Loads segments from disk into the provided params.segments.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded.
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   *
   * @param params The parameters for the log being loaded from disk
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(params: LoadLogParams): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 按照日志段文件名中的位移值正序排列 然后遍历文件
    for (file <- params.dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // 如果是索引文件
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(params.dir, offset)
        if (!logFile.exists) {
          // 对应的日志文件若不存在 记录一个警告并删除此索引文件
          warn(s"${params.logIdentifier} Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // 如果是日志文件
        // if it's a log file, load the corresponding log segment
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(params.dir, baseOffset).exists()
        // 创建对应的LogSegment对象 加入segments中
        val segment = LogSegment.open(
          dir = params.dir,
          baseOffset = baseOffset,
          params.config,
          time = params.time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"${params.logIdentifier} Could not find offset index file corresponding to log file" +
              s" ${segment.log.file.getAbsolutePath}, recovering segment and rebuilding index files...")
            recoverSegment(segment, params)
          case e: CorruptIndexException =>
            warn(s"${params.logIdentifier} Found a corrupted index file corresponding to log file" +
              s" ${segment.log.file.getAbsolutePath} due to ${e.getMessage}}, recovering segment and" +
              " rebuilding index files...")
            recoverSegment(segment, params)
        }
        params.segments.add(segment)
      }
    }
  }

  /**
   * Just recovers the given segment, without adding it to the provided params.segments.
   *
   * @param segment Segment to recover
   * @param params The parameters for the log being loaded from disk
   *
   * @return The number of bytes truncated from the segment
   *
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment, params: LoadLogParams): Int = {
    val producerStateManager = new ProducerStateManager(params.topicPartition, params.dir, params.maxProducerIdExpirationMs)
    Log.rebuildProducerState(
      producerStateManager,
      params.segments,
      params.logStartOffsetCheckpoint,
      segment.baseOffset,
      params.config.messageFormatVersion.recordVersion,
      params.time,
      reloadFromCleanShutdown = false)
    val bytesTruncated = segment.recover(producerStateManager, params.leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * This method completes any interrupted swap operations. In order to be crash-safe, the log files
   * that are replaced by the swap segment should be renamed to .deleted before the swap file is
   * restored as the new segment file.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only
   * called before all logs are loaded.
   *
   * @param swapFiles the set of swap
   * @param params The parameters for the log being loaded from disk
   *
   * @throws LogSegmentOffsetOverflowException if the swap file contains messages that cause the log segment offset to
   *                                           overflow. Note that this is currently a fatal exception as we do not have
   *                                           a way to deal with it. The exception is propagated all the way up to
   *                                           KafkaServer#startup which will cause the broker to shut down if we are in
   *                                           this situation. This is expected to be an extremely rare scenario in practice,
   *                                           and manual intervention might be required to get out of it.
   */
  private def completeSwapOperations(swapFiles: Set[File],
                                     params: LoadLogParams): Unit = {
    // 遍历所有.swap文件
    for (swapFile <- swapFiles) {
      // 获取对应的日志文件
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, Log.SwapFileSuffix, ""))
      // 获取日志文件起始的位移值
      val baseOffset = Log.offsetFromFile(logFile)
      // 创建对应的LogSegment对象
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        params.config,
        time = params.time,
        fileSuffix = Log.SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      // 进行日志段恢复操作
      recoverSegment(swapSegment, params)

      // We create swap files for two cases:
      // (1) Log cleaning where multiple segments are merged into one, and
      // (2) Log splitting where one segment is split into multiple.
      //
      // Both of these mean that the resultant swap segments be composed of the original set, i.e. the swap segment
      // must fall within the range of existing segment(s). If we cannot find such a segment, it means the deletion
      // of that segment was successful. In such an event, we should simply rename the .swap to .log without having to
      // do a replace with an existing segment.
      // 确认之前删除日志段是否成功 是否还存在旧的日志段文件
      val oldSegments = params.segments.values(swapSegment.baseOffset, swapSegment.readNextOffset).filter { segment =>
        segment.readNextOffset > swapSegment.baseOffset
      }
      // 若存在直接把.swap文件重命名为.log
      Log.replaceSegments(
        params.segments,
        Seq(swapSegment),
        oldSegments.toSeq,
        isRecoveredSwapFile = true,
        params.dir,
        params.topicPartition,
        params.config,
        params.scheduler,
        params.logDirFailureChannel,
        params.producerStateManager)
    }
  }

  /**
   * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
   * active segment, and returns the updated recovery point and next offset after recovery. Along
   * the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager inside
   * the provided LogComponents.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only
   * called before all logs are loaded.
   *
   * @param params The parameters for the log being loaded from disk
   *
   * @return a tuple containing (newRecoveryPoint, nextOffset).
   *
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private[log] def recoverLog(params: LoadLogParams): (Long, Long) = {
    /** return the log end offset if valid */
    def deleteSegmentsIfLogStartGreaterThanLogEnd(): Option[Long] = {
      if (params.segments.nonEmpty) {
        val logEndOffset = params.segments.lastSegment.get.readNextOffset
        if (logEndOffset >= params.logStartOffsetCheckpoint)
          Some(logEndOffset)
        else {
          warn(s"Deleting all segments because logEndOffset ($logEndOffset) is smaller than logStartOffset ${params.logStartOffsetCheckpoint}. " +
            "This could happen if segment files were deleted from the file system.")
          // 日志段集合不为空 验证分区日志的LEO值是否 < LogStartOffset 小于删除这些日志段对象
          removeAndDeleteSegmentsAsync(params.segments.values, params)
          params.leaderEpochCache.foreach(_.clearAndFlush())
          params.producerStateManager.truncateFullyAndStartAt(params.logStartOffsetCheckpoint)
          None
        }
      } else None
    }

    // If we have the clean shutdown marker, skip recovery.
    if (!params.hadCleanShutdown) {
      // 如果不存在以.kafka_cleanshutdown结尾的文件(通常都不存在)
      // 获取上次恢复点以外的所有unflushed日志段对象
      val unflushed = params.segments.values(params.recoveryPointCheckpoint, Long.MaxValue).iterator
      var truncated = false
      // 遍历这些unflushed日志段对象
      while (unflushed.hasNext && !truncated) {
        val segment = unflushed.next()
        info(s"${params.logIdentifier} Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            // 恢复操作
            recoverSegment(segment, params)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn(s"${params.logIdentifier} Found invalid offset during recovery. Deleting the" +
                s" corrupt segment and creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"${params.logIdentifier} Corruption found in segment ${segment.baseOffset}," +
            s" truncating to offset ${segment.readNextOffset}")
          // 如果有无效的消息导致被截断的字节数不为0 直接删除剩余的日志段对象
          removeAndDeleteSegmentsAsync(unflushed.toList, params)
          truncated = true
        }
      }
    }
    // 日志段集合不为空 验证分区日志的LEO值是否 < LogStartOffset 小于删除这些日志段对象
    val logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd()

    if (params.segments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      // 日志段集合为空 创建一个新的日志段 以logStartOffset为日志段的起始位移 并加入日志段集合中
      params.segments.add(
        LogSegment.open(
          dir = params.dir,
          baseOffset = params.logStartOffsetCheckpoint,
          params.config,
          time = params.time,
          initFileSize = params.config.initFileSize,
          preallocate = params.config.preallocate))
    }

    // Update the recovery point if there was a clean shutdown and did not perform any changes to
    // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
    // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
    // the recovery point when the log is flushed. If we advanced the recovery point here, we could
    // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
    // point and before we flush the segment.
    // 更新上一次恢复点属性并返回
    (params.hadCleanShutdown, logEndOffsetOption) match {
      case (true, Some(logEndOffset)) =>
        (logEndOffset, logEndOffset)
      case _ =>
        val logEndOffset = logEndOffsetOption.getOrElse(params.segments.lastSegment.get.readNextOffset)
        (Math.min(params.recoveryPointCheckpoint, logEndOffset), logEndOffset)
    }
  }

  /**
   * This method deletes the given log segments and the associated producer snapshots, by doing the
   * following for each of them:
   *  - It removes the segment from the segment map so that it will no longer be used for reads.
   *  - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
   *    synchronization and without the possibility of physically deleting a file while it is being
   *    read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either
   * called before all logs are loaded or the immediate caller will catch and handle IOException
   *
   * @param segmentsToDelete The log segments to schedule for deletion
   * @param params The parameters for the log being loaded from disk
   */
  private def removeAndDeleteSegmentsAsync(segmentsToDelete: Iterable[LogSegment],
                                           params: LoadLogParams): Unit = {
    if (segmentsToDelete.nonEmpty) {
      // As most callers hold an iterator into the `params.segments` collection and
      // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment, we should force
      // materialization of the iterator here, so that results of the iteration remain valid and
      // deterministic.
      val toDelete = segmentsToDelete.toList
      info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
      toDelete.foreach { segment =>
        params.segments.remove(segment.baseOffset)
      }
      Log.deleteSegmentFiles(
        segmentsToDelete,
        asyncDelete = true,
        deleteProducerStateSnapshots = true,
        params.dir,
        params.topicPartition,
        params.config,
        params.scheduler,
        params.logDirFailureChannel,
        params.producerStateManager)
    }
  }
}
