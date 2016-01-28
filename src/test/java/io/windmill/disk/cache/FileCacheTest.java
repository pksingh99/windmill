package io.windmill.disk.cache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import io.windmill.core.AbstractTest;
import io.windmill.utils.Futures;
import org.junit.Assert;
import org.junit.Test;

public class FileCacheTest extends AbstractTest
{
    @Test
    public void testGetAndCreate() throws Throwable
    {
        FileCache cache = new FileCache(CPUs.get(0), generateTmpFile(513 * Page.PAGE_SIZE));
        IntObjectMap<Page> existingPages = new IntObjectHashMap<>();

        try
        {
            for (int i = 0; i < 512; i++)
            {
                int pageOffset = ThreadLocalRandom.current().nextInt(0, 512);
                Page page = Futures.await(cache.getOrCreate(pageOffset));

                Assert.assertNotNull(page);

                // was already allocated once, verify it's the same page
                if (existingPages.containsKey(pageOffset))
                    Assert.assertEquals(page, existingPages.get(pageOffset));

                existingPages.put(pageOffset, page);
            }
        }
        finally
        {
            Futures.await(cache.close());
        }
    }

    @Test
    public void testSync() throws Throwable
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        FileChannel file = generateTmpFile(0);
        FileCache cache = new FileCache(CPUs.get(0), file);

        try
        {
            int newPages = 5;
            List<byte[]> pages = new ArrayList<>();

            for (int i = 0; i < newPages; i++)
            {
                byte[] buffer = new byte[random.nextInt(1, Page.PAGE_SIZE)];
                random.nextBytes(buffer);

                Page page = Futures.await(cache.getOrCreate(i)); // next page
                // page should be completely empty
                Assert.assertEquals(0, page.read((short) 0, Page.PAGE_SIZE).readableBytes());

                // write random data into the empty page
                page.write((short) 0, Unpooled.wrappedBuffer(buffer));
                pages.add(buffer);
            }

            int flushedPages = Futures.await(cache.sync());
            long expectedFileSize = flushedPages * Page.PAGE_SIZE - (Page.PAGE_SIZE - pages.get(newPages - 1).length);

            Assert.assertEquals(newPages, flushedPages);
            Assert.assertEquals(expectedFileSize, file.size());

            for (int i = 0; i < newPages; i++)
            {
                ByteBuf buffer = Unpooled.wrappedBuffer(pages.get(i));
                Page page = Futures.await(cache.getOrCreate(i));

                Assert.assertEquals(buffer, page.read((short) 0, Page.PAGE_SIZE));
            }
        }
        finally
        {
            Futures.await(cache.close());
        }
    }

    @Test
    public void testCacheMarking() throws Throwable
    {
        int numPages = 12288;
        FileCache cache = new FileCache(CPUs.get(0), generateTmpFile(numPages * Page.PAGE_SIZE));
        IntObjectMap<Page> dirtyPages = new IntObjectHashMap<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // get couple of random pages and dirty them
        for (int i = 0; i < 512; i++)
        {
            int pageOffset = random.nextInt(0, 12288);
            short pagePosition = (short) random.nextInt(0, Page.PAGE_SIZE - 1);

            Page page = Futures.await(cache.getOrCreate(pageOffset));

            // just write a single byte to a page, which should make it dirty
            page.write(pagePosition, Unpooled.buffer(1).writeByte('a'));
            dirtyPages.put(pageOffset, page);
        }

        Assert.assertTrue(cache.isDirty());
        List<Page> pages = cache.getDirtyPages();
        Assert.assertEquals(dirtyPages.size(), pages.size());
        pages.forEach((p) -> Assert.assertTrue(dirtyPages.containsValue(p)));

        int[] pageOffsets = dirtyPages.keys();
        for (int i = 0; i < 10; i++)
        {
            int index = random.nextInt(0, dirtyPages.size());
            Page page = dirtyPages.remove(pageOffsets[index]);

            if (page == null) // already cleaned
                continue;

            cache.markPageClean(pageOffsets[index]);
        }


        Assert.assertTrue(cache.isDirty());
        pages = cache.getDirtyPages();
        Assert.assertEquals(dirtyPages.size(), pages.size());

        // now let's mark the rest of the pages as clean and verify that global tree state changes
        for (int pageOffset : dirtyPages.keys())
            cache.markPageClean(pageOffset);

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        Futures.await(cache.close());

        // let's just see if a single page in the cache is going to be properly marked as dirty/clean
        cache = new FileCache(CPUs.get(2), generateTmpFile(Page.PAGE_SIZE));
        Page page = Futures.await(cache.getOrCreate(0));
        page.write((short) 0, Unpooled.buffer(1).writeByte('b'));

        Assert.assertTrue(cache.isDirty());
        Assert.assertEquals(page, cache.getDirtyPages().get(0));

        cache.markPageClean(0);

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        Futures.await(cache.close());

        // let's try to mark random pages in the empty tree
        cache = new FileCache(CPUs.get(0), generateTmpFile(0));

        for (int i = 0; i < 512; i++)
            cache.markPageDirty(random.nextInt(0, 12228));

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        for (int i = 0; i < 512; i++)
            cache.markPageClean(random.nextInt(0, 12228));

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        Futures.await(cache.close());
    }

    private static FileChannel generateTmpFile(long fileLength) throws IOException
    {
        File tmp = File.createTempFile("random-file-cache-", ".db");
        tmp.deleteOnExit();

        RandomAccessFile file = new RandomAccessFile(tmp, "rw");

        while (fileLength > 0)
        {
            int page = (int) Math.min(fileLength, Page.PAGE_SIZE);
            byte[] buffer = new byte[page];

            ThreadLocalRandom.current().nextBytes(buffer);
            file.write(buffer);

            fileLength -= page;
        }

        return file.getChannel();
    }
}
