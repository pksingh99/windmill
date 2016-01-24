package io.windmill.io.cache;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.buffer.Unpooled;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import org.junit.Assert;
import org.junit.Test;

public class RadixTreeTest
{
    @Test
    public void testGetAndCreate()
    {
        RadixTree cache = new RadixTree();
        IntObjectMap<Page> existingPages = new IntObjectHashMap<>();

        for (int i = 0; i < 512; i++)
        {
            int pageOffset = ThreadLocalRandom.current().nextInt(0, 512);
            Page page = cache.getOrCreate(pageOffset);

            Assert.assertNotNull(page);

            if (existingPages.containsKey(pageOffset)) // was already allocated once, verify it's the same page
                Assert.assertEquals(page, existingPages.get(pageOffset));

            existingPages.put(pageOffset, page);
        }
    }

    @Test
    public void testCacheMarking()
    {
        RadixTree cache = new RadixTree();
        IntObjectMap<Page> dirtyPages = new IntObjectHashMap<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // get couple of random pages and dirty them
        for (int i = 0; i < 512; i++)
        {
            int pageOffset = random.nextInt(0, 12288);
            short pagePosition = (short) random.nextInt(0, Page.PAGE_SIZE - 1);

            Page page = cache.getOrCreate(pageOffset);

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

        // let's just see if a single page in the cache is going to be properly marked as dirty/clean
        cache = new RadixTree();
        Page page = cache.getOrCreate(0);
        page.write((short) 0, Unpooled.buffer(1).writeByte('b'));

        Assert.assertTrue(cache.isDirty());
        Assert.assertEquals(page, cache.getDirtyPages().get(0));

        cache.markPageClean(0);

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        // let's try to mark random pages in the empty tree
        cache = new RadixTree();

        for (int i = 0; i < 512; i++)
            cache.markPageDirty(random.nextInt(0, 12228));

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());

        for (int i = 0; i < 512; i++)
            cache.markPageClean(random.nextInt(0, 12228));

        Assert.assertFalse(cache.isDirty());
        Assert.assertEquals(0, cache.getDirtyPages().size());
    }
}
