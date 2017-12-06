package org.corfudb.util;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import javax.annotation.Nonnull;

public class MappedByteBufInputStream extends
    ByteBufInputStream implements AutoCloseable {

    final AutoCleanableMappedBuffer buffer;

    /** Generate a new {@link MappedByteBufInputStream} over the entire file.
     *
     * @param fc                The FileChannel to map the stream over.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc)
        throws IOException {
        this(new AutoCleanableMappedBuffer(fc.map(MapMode.READ_ONLY, 0, fc.size())));
    }

    /** Given an existing {@link AutoCleanableMappedBuffer}, generate a new
     * {@link MappedByteBufInputStream} over it.
     * @param buf               The {@link AutoCleanableMappedBuffer} to base this stream off of.
     */
    private MappedByteBufInputStream(@Nonnull AutoCleanableMappedBuffer buf) {
        super(Unpooled.wrappedBuffer(buf.getBuffer()));
        this.buffer = buf;
    }

    /** Return a limited version of this {@link MappedByteBufInputStream} which only returns
     * {@code limit} bytes.
     *
     * @param limit             The number of bytes to limit to.
     * @return                  An {@link InputStream} which only permits reading up to the
     *                          given number of bytes.
     */
    public InputStream limited(int limit) {
        return ByteStreams.limit(this, limit);
    }

    /** {@inheritDoc}
     *
     *  Also closes the underlying {@link AutoCleanableMappedBuffer}.
     */
    @Override
    public void close() {
        buffer.close();
    }
}
