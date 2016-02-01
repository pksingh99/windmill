package io.windmill.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.windmill.core.CPUSet;
import io.windmill.core.Future;
import io.windmill.disk.cache.Page;
import io.windmill.disk.cache.PageCacheTest;
import io.windmill.disk.cache.PageCacheTest.CountingPageConsumer;
import io.windmill.net.Channel;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;
import io.windmill.utils.Futures;

import com.github.benmanes.caffeine.cache.Cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.*;

public class FileTest
{
    private static CPUSet CPU_SET;
    private static io.windmill.core.CPU CPU;

    @BeforeClass
    public static void beforeAll()
    {
        CPU_SET = CPUSet.builder().addSocket(0).build();
        CPU_SET.start();

        CPU = CPU_SET.get(0);
    }

    @AfterClass
    public static void afterAll()
    {
        CPU_SET.halt();
    }

    @Test
    public void testSuccessfulOpen() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        Future<File> fileFuture = CPU.open(createTempFile("abc"), "rw");
        fileFuture.onSuccess((f) -> {
            latch.countDown();
            f.close();
        });

        Futures.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS);
        Assert.assertTrue(fileFuture.isSuccess());
    }

    @Test
    public void testFailingOpen() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IOException> exception = new AtomicReference<>();
        Future<File> fileFuture = CPU.open("/tmp/non-existent-akane", "r");
        fileFuture.onFailure((e) -> {
            latch.countDown();
            exception.set((IOException) e);
        });

        Futures.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS);

        Assert.assertTrue(fileFuture.isFailure());
        Assert.assertEquals(FileNotFoundException.class, exception.get().getClass());
    }

    @Test
    public void testingBasicRW() throws Throwable
    {
        String str = "hello world";
        java.io.File tmp = createTempFile("akane-io");
        File file = Futures.await(CPU.open(tmp, "rw"));
        ThreadLocalRandom random = ThreadLocalRandom.current();

        try
        {
            Assert.assertEquals(4L, Futures.await(file.write(0, getInt(str.length()))).getPosition());
            Assert.assertEquals(4L + str.length(), Futures.await(file.write(4, str.getBytes())).getPosition());

            // flushed a single page
            Assert.assertEquals(1, (int) Futures.await(file.sync()));

            // let's try positional reads first
            Assert.assertEquals(str.length(), Futures.await(file.read(0, 4)).readInt());
            Assert.assertArrayEquals(str.getBytes(), Futures.await(file.read(4, str.length())).array());

            // let's try to read from non-existent page
            Assert.assertEquals(Unpooled.EMPTY_BUFFER, Futures.await(file.read(31337, 2 * Page.PAGE_SIZE)));

            // seek back to the start and read the message back
            ByteBuf helloWorld = Futures.await(file.seek(0).flatMap((context) -> context.read(4).flatMap((header) -> {
                int len = header.readInt();
                Assert.assertEquals(str.length(), len);
                return context.read(len);
            })));

            Assert.assertEquals(str, new String(helloWorld.array()));

            // allocate multi-page buffer and try to write (re-writing previous data)
            byte[] buffer = new byte[3 * Page.PAGE_SIZE];
            random.nextBytes(buffer);

            Assert.assertEquals(buffer.length, Futures.await(file.seek(0).flatMap((context) -> context.write(buffer))).getPosition());
            Assert.assertEquals(4,  (int) Futures.await(file.sync()));
            Assert.assertEquals(buffer.length, tmp.length());

            List<byte[]> pages = new ArrayList<>();
            for (int i = 0; i < 3 * Page.PAGE_SIZE; i += Page.PAGE_SIZE * 2)
            {
                byte[] bytes = new byte[random.nextInt(1, Page.PAGE_SIZE)];
                random.nextBytes(bytes);

                Futures.await(file.seek(i).flatMap((context) -> context.write(bytes)));
                pages.add(bytes);
            }

            Assert.assertEquals(2, (int) Futures.await(file.sync()));

            int pageOffset = 0;
            for (byte[] bytes : pages)
            {
                ByteBuf read = Futures.await(file.seek(pageOffset).flatMap((context) -> context.read(bytes.length)));
                Assert.assertEquals(Unpooled.wrappedBuffer(bytes), read);
                pageOffset += Page.PAGE_SIZE * 2;
            }
        }
        finally
        {
            Futures.await(file.close());
        }
    }

    @Test
    public void testFileTransfer() throws Throwable
    {
        byte[] randomBytes = new byte[Page.PAGE_SIZE * 5];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        ByteBuf buffer = Unpooled.wrappedBuffer(randomBytes);
        File file = Futures.await(CPU.open(createTempFile("transferTo"), "rw"));

        try
        {
            Assert.assertEquals(buffer.readableBytes(), Futures.await(file.write(0, buffer)).getPosition());
            Assert.assertEquals(6,  (int) Futures.await(file.sync()));

            // evict random page to test situation when there are holes in the cache
            Futures.await(file.cache.evictPage(ThreadLocalRandom.current().nextInt(1, 4)));

            CPU.listen(new InetSocketAddress("127.0.0.1", 31338)).onSuccess((c) -> {
                InputStream in = c.getInput();
                c.loop((cpu, prev) -> in.read(8).map((header) -> file.transferTo(c, header.readInt(), header.readInt())));
            });

            Future<Channel> client = CPU.connect(new InetSocketAddress("127.0.0.1", 31338));

            CountDownLatch latch = new CountDownLatch(100);
            AtomicBoolean result = new AtomicBoolean(true);

            client.onSuccess((channel) -> {
                InputStream in = channel.getInput();
                OutputStream out = channel.getOutput();

                channel.loop((cpu, prev) -> {
                    if (latch.getCount() == 0) // done
                        return Futures.failedFuture(cpu, null);

                    int offset = ThreadLocalRandom.current().nextInt(0, randomBytes.length - 2);
                    int length = ThreadLocalRandom.current().nextInt(1, randomBytes.length - offset);

                    // write offset and length
                    out.writeAndFlush(Unpooled.buffer(16).writeInt(offset).writeInt(length));

                    return in.read(length).map((region) -> {
                        try
                        {
                            boolean isMatch = buffer.slice(offset, length).equals(region);
                            if (!isMatch)
                                System.out.println("!!! failure at offset " + offset + ", length = " + length);
                            result.set(result.get() & isMatch);
                            return Futures.voidFuture(cpu);
                        }
                        finally
                        {
                            latch.countDown();
                        }
                    });
                });
            });

            client.onFailure((e) -> {
                e.printStackTrace();

                // drain latch in case of failure
                while (latch.getCount() != 0)
                    latch.countDown();
            });

            Futures.awaitUninterruptibly(latch);

            // all of the regions should be valid to comply
            Assert.assertTrue(result.get());
        }
        finally
        {
            Futures.await(file.close());
        }
    }

    @Test
    public void testPageTracker() throws Throwable
    {
        int numPages = 3;
        File file = Futures.await(CPU.open(PageCacheTest.generateTmpFile(numPages * Page.PAGE_SIZE), "rw"));
        Cache<PageRef, Boolean> pageTracker = file.ioService.pageTracker;

        // let's invalidate everything currently in the tracker
        pageTracker.invalidateAll();

        // and now re-fault all pages we have
        for (int i = 0; i < numPages; i++)
            Futures.await(file.read(i * Page.PAGE_SIZE, 1));

        Assert.assertEquals(3, pageTracker.estimatedSize());

        int evictedOffset = ThreadLocalRandom.current().nextInt(0, numPages);
        // let's evict one page and see if read from file brings it back up
        pageTracker.invalidate(new PageRef(file, evictedOffset));

        // because we can't track when page is actually evicted since it's async
        Futures.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, pageTracker.estimatedSize());

        CountingPageConsumer pageConsumer = new CountingPageConsumer();
        file.cache.forEach(pageConsumer);
        Assert.assertEquals(2, pageConsumer.getCount());

        Futures.await(file.read(evictedOffset * Page.PAGE_SIZE, 1));

        Assert.assertEquals(3, pageTracker.estimatedSize());
        pageConsumer = new CountingPageConsumer();
        file.cache.forEach(pageConsumer);
        Assert.assertEquals(3, pageConsumer.getCount());

        // should evict everything from page tracker
        Futures.await(file.close());

        Assert.assertEquals(0, pageTracker.estimatedSize());
        pageConsumer = new CountingPageConsumer();
        file.cache.forEach(pageConsumer);
        Assert.assertEquals(0, pageConsumer.getCount());
    }

    private byte[] getInt(int n)
    {
        return Unpooled.copyInt(n).array();
    }

    private java.io.File createTempFile(String prefix) throws IOException
    {
        java.io.File f = java.io.File.createTempFile(prefix, "db");
        f.deleteOnExit();
        return f;
    }
}
