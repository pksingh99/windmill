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
        RandomAccessFile file = createTempFile("page-rw", "rw");

        try
        {
            PageCache cache = new PageCache(CPUs.get(0), file.getChannel());
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
            file = createTempFile("page", "rw");
            file.write(bytes);

            PageCache cache = new PageCache(CPUs.get(0), file.getChannel());

            Page page = Futures.await(cache.getOrCreate(0));

            Assert.assertEquals(buffer, page.read((short) 0, bytes.length));
            Assert.assertEquals(buffer, page.read((short) 0, Page.PAGE_SIZE));

            // let's add some bytes to the page now and re-read it
            byte[] suffix = new byte[random.nextInt(1, 512)];
            random.nextBytes(suffix);

            page.write((short) bytes.length, Unpooled.wrappedBuffer(suffix));
            page.writeTo(file.getChannel(), true);

            cache = new PageCache(CPUs.get(0), file.getChannel());
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

    @Test
    public void testBlockMarking() throws Throwable
    {
        RandomAccessFile file = createTempFile("page-marking", "rw");

        byte[] bytes = new byte[256];
        ThreadLocalRandom.current().nextBytes(bytes);

        file.write(bytes);

        PageCache cache = new PageCache(CPUs.get(0), file.getChannel());

        try
        {
            Page page = Futures.await(cache.getOrCreate(0));

            byte[] suffix = new byte[389];
            ThreadLocalRandom.current().nextBytes(suffix);

            byte[] independent = new byte[768];
            ThreadLocalRandom.current().nextBytes(independent);

            page.write((short) bytes.length, Unpooled.wrappedBuffer(suffix));
            page.write((short) 1024, Unpooled.wrappedBuffer(independent));
            Assert.assertTrue(page.isDirty());

            page.writeTo(file.getChannel(), true);
            Assert.assertFalse(page.isDirty());

            cache = new PageCache(CPUs.get(0), file.getChannel());
            Page extendedPage = Futures.await(cache.getOrCreate(0));

            Assert.assertEquals(page.read((short) 0, 645), extendedPage.read((short) 0, 645));
            Assert.assertEquals(page.read((short) 1024, independent.length), extendedPage.read((short) 1024, independent.length));
            Assert.assertEquals(page.read((short) 0, Page.PAGE_SIZE), extendedPage.read((short) 0, Page.PAGE_SIZE));
            Assert.assertEquals(1792, page.read((short) 0, Page.PAGE_SIZE).readableBytes());
        }
        finally
        {
            Futures.await(cache.close());
        }
    }

    @Test
    public void testIntersectingPageReads() throws Throwable
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        PageCache cache = new PageCache(CPUs.get(0), createTempFile("page-reads", "rw").getChannel());

        try
        {
            Page page = Futures.await(cache.getOrCreate(0));

            byte[] a = new byte[15];
            random.nextBytes(a);

            byte[] b = new byte[10];
            random.nextBytes(b);

            page.write((short) 0, Unpooled.wrappedBuffer(a));
            page.write((short) 0, Unpooled.wrappedBuffer(b));

            Assert.assertEquals(Unpooled.wrappedBuffer(b), page.read((short) 0, b.length));
            Assert.assertEquals(15, page.read((short) 0, a.length).readableBytes());

            byte[] c = new byte[12];
            random.nextBytes(c);

            page.write((short) 10, Unpooled.wrappedBuffer(c));

            int expected = 10 + c.length;

            Assert.assertEquals(expected, page.read((short) 0, expected).readableBytes());
            Assert.assertEquals(Unpooled.wrappedBuffer(b), page.read((short)  0, b.length));
            Assert.assertEquals(Unpooled.wrappedBuffer(c), page.read((short) 10, c.length));
        }
        finally
        {
            Futures.await(cache.close());
        }
    }

    private static RandomAccessFile createTempFile(String prefix, String mode) throws IOException
    {
        File tmp = File.createTempFile(prefix, ".tmp");
        tmp.deleteOnExit();

        return new RandomAccessFile(tmp, mode);
    }
}
