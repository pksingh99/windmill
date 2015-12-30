package com.apple.akane.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.apple.akane.core.CPU;
import com.apple.akane.core.CPUSet;
import com.apple.akane.core.Future;
import com.apple.akane.core.tasks.Task0;

import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.*;

public class FileTest
{
    private static CPUSet CPU_SET;
    private static CPU CPU;

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
