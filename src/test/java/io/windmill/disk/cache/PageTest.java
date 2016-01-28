package io.windmill.disk.cache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;

import io.windmill.core.AbstractTest;
import io.windmill.utils.Futures;
import io.windmill.utils.IOUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Assert;
import org.junit.Test;

public class PageTest extends AbstractTest
{
    @Test
    public void testRW() throws IOException
    {
        File tmp = File.createTempFile("page-rw", ".db");
        tmp.deleteOnExit();

        RandomAccessFile file = new RandomAccessFile(tmp, "rw");

        try
        {
            FileCache cache = new FileCache(CPUs.get(0), file.getChannel());
            Page page = new Page(cache, 0, Unpooled.buffer(Page.PAGE_SIZE));

            byte[] bytes = new byte[128];
            ThreadLocalRandom.current().nextBytes(bytes);
            ByteBuf buffer = Unpooled.wrappedBuffer(bytes);

            Assert.assertEquals(bytes.length, page.write((short) 0, buffer.duplicate()));
            Assert.assertEquals(100, page.write((short) (Page.PAGE_SIZE - 100), buffer.duplicate()));

            Assert.assertEquals(buffer, page.read((short) 0, bytes.length));
            Assert.assertEquals(buffer.slice(0, 100), page.read((short) (Page.PAGE_SIZE - 100), Integer.MAX_VALUE));
        }
        finally
        {
            IOUtils.closeQuietly(file);
        }
    }

    @Test
    public void testRWFile() throws Throwable
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        byte[] bytes = new byte[256];
        random.nextBytes(bytes);
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);

        RandomAccessFile file = null;

        try
        {

            File tmp = File.createTempFile("page", ".tmp");
            tmp.deleteOnExit();

            file = new RandomAccessFile(tmp, "rw");

            file.write(bytes);

            FileCache cache = new FileCache(CPUs.get(0), file.getChannel());

            Page page = Futures.await(cache.getOrCreate(0));

            Assert.assertEquals(buffer, page.read((short) 0, bytes.length));
            Assert.assertEquals(buffer, page.read((short) 0, Page.PAGE_SIZE));

            // let's add some bytes to the page now and re-read it
            byte[] suffix = new byte[random.nextInt(1, 512)];
            random.nextBytes(suffix);

            page.write((short) bytes.length, Unpooled.wrappedBuffer(suffix));
            page.writeTo(file.getChannel());

            cache = new FileCache(CPUs.get(0), file.getChannel());
            Page extendedPage = Futures.await(cache.getOrCreate(0));

            int totalSize = bytes.length + suffix.length;

            Assert.assertEquals(page.read((short) 0, totalSize), extendedPage.read((short) 0, totalSize));

            // let's get some random slices and validate them
            for (int i = 0; i < 100; i++)
            {
                short offset = (short) random.nextInt(0, totalSize / 2);
                int size = random.nextInt(1, Page.PAGE_SIZE - offset);

                Assert.assertEquals(page.read(offset, size), extendedPage.read(offset, size));
            }

            // try to read something beyond watermark
            Assert.assertEquals(0, page.read((short) (totalSize + 10), Page.PAGE_SIZE).readableBytes());
        }
        finally
        {
            IOUtils.closeQuietly(file);
        }
    }
}
