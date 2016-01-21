package io.windmill.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.windmill.core.CPUSet;
import io.windmill.core.Future;
import io.windmill.core.tasks.Task0;
import io.windmill.net.Channel;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;
import io.windmill.utils.Futures;

import com.google.common.util.concurrent.Uninterruptibles;

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

        Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS);
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

        Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS);

        Assert.assertTrue(fileFuture.isFailure());
        Assert.assertEquals(FileNotFoundException.class, exception.get().getClass());
    }

    @Test
    public void testingBasicRW() throws Exception
    {
        String str = "hello world";
        CountDownLatch writeLatch = new CountDownLatch(1);
        AtomicReference<File> fileRef = new AtomicReference<>();

        CPU.open(createTempFile("akane-io"), "rw")
                .onSuccess((f) -> {
                    fileRef.set(f);
                    f.write(getInt(str.length()))
                            .onSuccess((v) -> f.write(str.getBytes())
                                    .onSuccess((w) -> writeLatch.countDown()));
                });

        Uninterruptibles.awaitUninterruptibly(writeLatch, 1, TimeUnit.SECONDS);

        Assert.assertNotNull(fileRef.get());

        File file = fileRef.get();

        executeBlocking(file::sync);
        executeBlocking(() -> file.seek(0)); // seek back to the beginning of the file

        ByteBuf helloWorld = executeBlocking(() -> file.read(4).flatMap((header) -> {
            int len = header.readInt();
            Assert.assertEquals(str.length(), len);
            return file.read(len);
        }));

        Assert.assertEquals(str, new String(helloWorld.array()));
        executeBlocking(file::close);
    }

    @Test
    public void testFileTransfer() throws Throwable
    {
        byte[] randomBytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        ByteBuf buffer = Unpooled.wrappedBuffer(randomBytes);
        File file = Futures.await(CPU.open(createTempFile("transferTo"), "rw"));

        try
        {
            executeBlocking(() -> file.write(buffer));
            executeBlocking(file::sync);

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
                            result.set(result.get() & buffer.slice(offset, length).equals(region));
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

            Uninterruptibles.awaitUninterruptibly(latch);

            // all of the regions should be valid to comply
            Assert.assertTrue(result.get());
        }
        finally
        {
            if (file != null)
                executeBlocking(file::close);
        }
    }

    private byte[] getInt(int n)
    {
        return Unpooled.copyInt(n).array();
    }

    private <O> O executeBlocking(Task0<Future<O>> task)
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<O> ref = new AtomicReference<>();

        task.compute().onSuccess((v) -> {
            latch.countDown();
            ref.set(v);
        });

        Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS);
        return ref.get();
    }

    private java.io.File createTempFile(String prefix) throws IOException
    {
        java.io.File f = java.io.File.createTempFile(prefix, "db");
        f.deleteOnExit();
        return f;
    }
}
