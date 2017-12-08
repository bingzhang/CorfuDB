package org.corfudb.integration;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {


    @Test
    public void testWrite() throws Exception {
        byte[] data = new byte[50 * 1000 * 1000];
        byte[] index = new byte[80000];

        String path1 = "/tmp/log1";
        String path2 = "/tmp/log2";

        FileChannel ch1 = FileChannel.open(FileSystems.getDefault().getPath(path1),
                EnumSet.of(StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));

        FileChannel ch2 = FileChannel.open(FileSystems.getDefault().getPath(path2),
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));

        ByteBuffer dataBuf = ByteBuffer.wrap(data);
        ByteBuffer indexBuf = ByteBuffer.wrap(index);

        int numWrites = 50;

        long duration = 0;
        for (int x = 0; x < numWrites; x++) {
            long startTime = System.nanoTime();
            ch1.write(dataBuf);
            ch1.force(true);
            long endTime = System.nanoTime();
            duration += endTime - startTime;
            dataBuf = ByteBuffer.wrap(data);
        }

        System.out.println((double) duration / numWrites);

        dataBuf = ByteBuffer.wrap(data);
        long duration2 = 0;
        for (int x = 0; x < numWrites; x++) {
            long startTime = System.nanoTime();
            long pos = ch2.position();
            ch2.position(0);
            ch2.write(indexBuf);
            ch2.position(pos);
            ch2.write(dataBuf);
            ch2.force(true);
            long endTime = System.nanoTime();
            duration2 += endTime - startTime;
            dataBuf = ByteBuffer.wrap(data);
            indexBuf = ByteBuffer.wrap(index);

        }

        System.out.println((double) duration2 / numWrites);
    }

    @Test
    public void compressionPerf() {
        final int numAddresses = 10_000;
        ByteBuffer buf = ByteBuffer.allocate(numAddresses * Long.BYTES);
        for (int x = numAddresses*2; x < numAddresses * 3; x++) {
            buf.putLong((long) x);
        }

        buf.flip();

        byte[] data = new byte[numAddresses * Long.BYTES];
        buf.get(data);

        final int decompressedLength = data.length;

        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        long startTime = System.nanoTime();
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        System.out.println("Size of data " + decompressedLength);
        System.out.println("Compressed size " + compressedLength);

        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[decompressedLength];
        long startTime2 = System.nanoTime();
        int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);
        long endTime2 = System.nanoTime();
        long duration2 = (endTime2 - startTime2);

        System.out.println("time " + duration + " " + duration2);
        System.out.println("decompress size " + compressedLength2);
    }

    @Test
    public void simpleStreamTest() throws Exception {

        final int numIter = 10000;
        final int payloadSize = 4000;
        final int numThreads = 4;

        Thread[] threads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            Runnable r = () -> {
                CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
                rt.setCacheDisabled(true);
                byte[] payload = new byte[payloadSize];
                for (int y = 0; y < numIter/numThreads; y++) {
                    rt.getStreamsView().get(CorfuRuntime.getStreamID("s1")).append(payload);
                }
            };

            threads[x] = new Thread(r);
        }

        long startTime = System.currentTimeMillis();
        for (int x = 0; x < numThreads; x++) {
            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
    }
}
