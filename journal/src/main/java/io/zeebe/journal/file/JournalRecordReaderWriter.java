/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.journal.file;

import com.esotericsoftware.kryo.KryoException;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.zeebe.journal.JournalRecord;
import io.zeebe.journal.StorageException;
import io.zeebe.journal.StorageException.InvalidChecksum;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Common methods used by {@link MappedJournalSegmentWriter } and {@link MappedJournalSegmentReader}
 * to write and read records to and from the buffer
 */
public class JournalRecordReaderWriter {

  private static final Namespace NAMESPACE =
      new Namespace.Builder()
          .register(Namespaces.BASIC)
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
          .register(PersistedJournalRecord.class)
          .register(UnsafeBuffer.class)
          .name("Journal")
          .build();
  private final CRC32 crc32 = new CRC32();
  private final int maxEntrySize;

  public JournalRecordReaderWriter(final int maxEntrySize) {
    this.maxEntrySize = maxEntrySize;
  }

  /**
   * Reads the JournalRecord in the buffer at the current position. After the methods returns, the
   * position of {@code buffer} will be advanced to the next record.
   */
  public JournalRecord read(final ByteBuffer buffer, final long expectedIndex) {
    // Mark the buffer so it can be reset if necessary.
    buffer.mark();

    try {
      // Read the length of the record.
      final int length = buffer.getInt();

      // If the buffer length is zero then return.
      if (length <= 0 || length > maxEntrySize) {
        buffer.reset();
        return null;
      }

      final ByteBuffer slice = buffer.slice();
      slice.limit(length);

      // If the stored checksum equals the computed checksum, return the record.
      slice.rewind();
      final PersistedJournalRecord record = NAMESPACE.deserialize(slice);
      final var checksum = record.checksum();
      // TODO: checksum should also include asqn.
      // TODO: It is now copying the data to calculate the checksum. This should be fixed.
      final var expectedChecksum = computeChecksum(record.data());
      if (checksum != expectedChecksum || expectedIndex != record.index()) {
        buffer.reset();
        return null;
      }
      buffer.position(buffer.position() + length);
      buffer.mark();
      return record;

    } catch (final BufferUnderflowException e) {
      buffer.reset();
    }
    return null;
  }

  private int computeChecksum(final DirectBuffer data) {
    final byte[] slice = new byte[data.capacity()];
    data.getBytes(0, slice);
    crc32.reset();
    crc32.update(slice);
    return (int) crc32.getValue();
  }

  /**
   * Create and writes a new JournalRecord with the given index,asqn and data to the buffer. After
   * the method returns, the position of buffer will be advanced to a position were the next record
   * will be written.
   */
  public JournalRecord write(
      final ByteBuffer buffer, final long index, final long asqn, final DirectBuffer data) {

    // compute checksum and construct the record
    // TODO: checksum should also include asqn. https://github.com/zeebe-io/zeebe/issues/6218
    // TODO: It is now copying the data to calculate the checksum. This should be fixed when
    // we change the serialization format. https://github.com/zeebe-io/zeebe/issues/6219
    final var checksum = computeChecksum(data);
    final var recordToWrite = new PersistedJournalRecord(index, asqn, checksum, data);

    writeInternal(buffer, recordToWrite);
    return recordToWrite;
  }

  /**
   * Write the record to the buffer. After the method returns, the position of buffer will be
   * advanced to a position were the next record will be written.
   */
  public JournalRecord write(final ByteBuffer buffer, final JournalRecord record) {
    final var checksum = computeChecksum(record.data());
    if (checksum != record.checksum()) {
      throw new InvalidChecksum("Checksum invalid for record " + record);
    }
    writeInternal(buffer, record);
    return record;
  }

  private void writeInternal(final ByteBuffer buffer, final JournalRecord recordToWrite) {
    final int recordStartPosition = buffer.position();
    buffer.mark();
    if (recordStartPosition + Integer.BYTES > buffer.limit()) {
      throw new BufferOverflowException();
    }

    buffer.position(recordStartPosition + Integer.BYTES);
    try {
      NAMESPACE.serialize(recordToWrite, buffer);
    } catch (final KryoException e) {
      buffer.reset();
      throw new BufferOverflowException();
    }

    final int length = buffer.position() - (recordStartPosition + Integer.BYTES);

    // If the entry length exceeds the maximum entry size then throw an exception.
    if (length > maxEntrySize) {
      // Just reset the buffer. There's no need to zero the bytes since we haven't written the
      // length or checksum.
      buffer.reset();
      throw new StorageException.TooLarge(
          "Entry size " + length + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }

    buffer.position(recordStartPosition);
    buffer.putInt(length);
    buffer.position(recordStartPosition + Integer.BYTES + length);
  }
}
