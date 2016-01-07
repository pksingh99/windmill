package io.windmill.core;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;

import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Assert;
import org.junit.Test;

public class CPUTest extends AbstractTest
{
    @Test
    public void testLoop()
    {
        CountDownLatch latch = new CountDownLatch(10);

        CPUs.get(0).loop((cpu) -> {
            try
            {
                return (latch.getCount() == 0)
                        ? new Future<Integer>(cpu) {{ setFailure(null); }}
                        : new ConstantFuture<>(cpu, 42);
            }
            finally
            {
                latch.countDown();
            }
        });

        Uninterruptibles.awaitUninterruptibly(latch);
    }

    @Test
    public void testListen() throws Exception
    {
        // non-blocking server which takes frame consisting
        CPUs.get(0).listen(new InetSocketAddress("localhost", 31337), (c) -> {
            InputStream input = c.getInput();
            OutputStream output = c.getOutput();

            c.loop((cpu) -> input.read(4).flatMap((header) -> input.read(header.readInt()))
                                         .onSuccess((msg) -> {
                                             int sum = 0;
                                             while (msg.readableBytes() > 0)
                                                 sum += msg.readInt();

                                             output.writeAndFlush(Unpooled.buffer(12).writeInt(sum))
                                                   .onSuccess((bytesWritten) -> Assert.assertEquals(4, bytesWritten.intValue()));
                                         }));
        }, Throwable::printStackTrace);


        try (Socket client = new Socket("localhost", 31337))
        {
            client.setTcpNoDelay(true);

            byte[] response = new byte[4];
            for (int i = 0; i < 10; i++)
            {
                ByteBuf request = getRequest(new int[] { i, i + 1, i + 2 });
                request.readBytes(client.getOutputStream(), request.readableBytes());

                java.io.InputStream in = client.getInputStream();

                Assert.assertEquals(4, in.read(response));
                Assert.assertEquals(3 * i + 3, Unpooled.wrappedBuffer(response).readInt());
            }

            client.close();
        }
    }

    @Test
    public void testSleep()
    {
        CPU cpu = CPUs.get(0);

        long now = System.nanoTime();
        AtomicInteger counts = new AtomicInteger(0);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < 5; i++)
        {
            long delay = random.nextInt(10, 50);
            cpu.sleep(delay, TimeUnit.MILLISECONDS, () -> {
                long n = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - now);
                Assert.assertTrue(n >= delay);
                return counts.incrementAndGet();
            });

            // also insert couple of empty tasks to make
            // sure that sleep works with other tasks around
            cpu.schedule(() -> 2 + 2);
        }

        cpu.sleep(500, TimeUnit.MILLISECONDS, counts::incrementAndGet);

        Uninterruptibles.sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
        Assert.assertEquals(5, counts.get());

        // sleep a bit for to get 6th count
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        Assert.assertEquals(6, counts.get());
    }

    private static ByteBuf getRequest(int[] numbers)
    {
        ByteBuf request = Unpooled.buffer(4 + numbers.length * 4);

        request.writeInt(request.capacity() - 4);
        for (int n : numbers)
            request.writeInt(n);

        return request;
    }
}
