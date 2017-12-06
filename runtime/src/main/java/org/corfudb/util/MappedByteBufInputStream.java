package org.corfudb.util;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import javax.annotation.Nonnull;

public class MappedByteBufInputStream extends
    ByteBufInputStream implements AutoCloseable {

    final AutoCleanableMappedBuffer buffer;
    final ByteBuf byteBuf;

    /** Generate a new {@link MappedByteBufInputStream} over the entire file.
     *  The file must be <2GB.
     *
     * @param fc                The FileChannel to map the stream over.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc)
        throws IOException {
        this(fc, fc.size());
    }

    /** Generate a new {@link MappedByteBufInputStream} over a file length.
     *  The length must be <2GB.
     * @param fc                The FileChannel to map the stream over.
     * @param length            The length of the file to map.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc, long length)
    throws IOException {
        // fc.map will throw an IOException _before_ the MappedBuffer is allocated
        // if length > 2GB.
        this(new AutoCleanableMappedBuffer(fc.map(MapMode.READ_ONLY, 0, length)));
    }

    /** Generate a new {@link MappedByteBufInputStream} over a file length and start position.
     *  The length must be <2GB.
     * @param fc                The FileChannel to map the stream over.
     * @param position          The position to start at.
     * @param length            The length of the file to map.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc, long position, long length)
        throws IOException {
        // fc.map will throw an IOException _before_ the MappedBuffer is allocated
        // if length > 2GB.
        this(new AutoCleanableMappedBuffer(fc.map(MapMode.READ_ONLY, position, length)));
    }

    /** Given an existing {@link AutoCleanableMappedBuffer}, generate a new
     * {@link MappedByteBufInputStream} over it.
     * @param buf               The {@link AutoCleanableMappedBuffer} to base this stream off of.
     */
    private MappedByteBufInputStream(@Nonnull AutoCleanableMappedBuffer buf) {
        this(Unpooled.wrappedBuffer(buf.getBuffer()), buf);
    }

    /** Given an existing {@link ByteBuf}, generate a new
     * {@link MappedByteBufInputStream} over it.
     * @param bb                The {@link ByteBuf} generated off of {@code buf},
     *                          created via a call to {@link Unpooled#wrappedBuffer(ByteBuffer)}
     * @param buf               The {@link AutoCleanableMappedBuffer} to base this stream off of.
     */
    private MappedByteBufInputStream(@Nonnull ByteBuf bb, @Nonnull AutoCleanableMappedBuffer buf) {
        super(bb);
        this.byteBuf = bb;
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

    /** Get the position of the reader into this stream.
     *
     * @return                  The position of this stream, in bytes.
     */
    public int position() {
        return byteBuf.readerIndex();
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
