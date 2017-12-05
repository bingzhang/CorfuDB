package org.corfudb.util;

import java.nio.MappedByteBuffer;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

/** {@link this} wraps around a {@link MappedByteBuffer}, implementing the
 *  {@link java.lang.AutoCloseable} interface which enables a {@link MappedByteBuffer} to be
 *  automatically cleaned via a try-resources block.
 *
 *  <p>Once the buffer is closed it should no longer be used.
 */
@Slf4j
public class AutoCleanableMappedBuffer implements AutoCloseable {

    /** The {@link java.nio.MappedByteBuffer} being wrapped. */
    final MappedByteBuffer buffer;

    /** Construct a new {@link org.corfudb.util.AutoCleanableMappedBuffer}.
     *
     * @param buffer    The {@link MappedByteBuffer} to wrap.
     */
    public AutoCleanableMappedBuffer(@Nonnull MappedByteBuffer buffer) {
        this.buffer = buffer;
    }

    /** Get the underlying buffer. It is recommended that a reference NOT be saved (e.g, call
     * {@link this#getBuffer()} each time the buffer is needed).
     *
     * @return The underlying {@link MappedByteBuffer}
     */
    public MappedByteBuffer getBuffer() {
        return buffer;
    }

    /** {@inheritDoc}
     *
     * @throws UnsupportedOperationException    If the buffer could not be auto-cleaned.
     */
    @Override
    public void close() {
        try {
            ((DirectBuffer) buffer).cleaner().clean();
        } catch (Exception ex) {
            throw new UnsupportedOperationException("Failed to autoclean buffer");
        }
    }
}
