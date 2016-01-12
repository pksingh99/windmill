package io.windmill;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import io.windmill.core.CPU;
import io.windmill.core.CPUSet;
import io.windmill.core.tasks.Task0;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;

import io.netty.buffer.Unpooled;

public class App 
{
    public static void main(String[] args) throws Exception
    {
        CPUSet cpus = CPUSet.builder().addSocket(0, 1).build();

        cpus.start();

        CPU cpu0 = cpus.get(0);
        CPU cpu1 = cpus.get(1);

        cpu0.schedule(sum(new int[] { 1, 2, 3 })).onSuccess(System.out::println);
        cpu1.schedule(sum(new int[] { 4, 5, 6 })).onSuccess(System.out::println);
        cpu0.schedule(sum(new int[] { 7, 8, 9 })).onSuccess(System.out::println);
        cpu1.schedule(sum(new int[] { 10, 11, 12 })).map((sum) -> Integer.toString(sum)).onSuccess(System.out::println);
        cpu0.schedule(sum(new int[] { 13, 14, 15 })).flatMap((sum) -> cpu1.schedule(() -> 42)).onSuccess(System.out::println);

        cpu0.listen(new InetSocketAddress("localhost", 31337), (c) -> {
            System.out.println("connected to => " + c + " on " + Thread.currentThread());

            InputStream input = c.getInput();
            OutputStream output = c.getOutput();

            AtomicInteger counter = new AtomicInteger(0);
            AtomicInteger totalSize = new AtomicInteger(0);

            c.loop((cpu) -> input.read(4).flatMap((header) -> input.read(header.readInt()))
                                         .map((msg) -> {
                                             int sum = 0;
                                             int count = msg.readInt();
                                             for (int i = 0; i < count; i++)
                                                 sum += msg.readInt();

                                             long timestamp = msg.readLong();

                                             totalSize.addAndGet(4 + 4 + count * 4 + 8);
                                             if (counter.incrementAndGet() % 10000 == 0)
                                                 System.out.println("received " + counter + " messages, total size " + totalSize.get() + " bytes");

                                             output.writeAndFlush(Unpooled.buffer(12).writeInt(sum).writeLong(timestamp));
                                             return null;
                                         }));

        }, Throwable::printStackTrace);

        cpu0.schedule(sum(new int[] { 10, 11, 12 })).map(cpu1, (sum) -> Integer.toString(sum)).onSuccess(System.out::println);

        Thread.currentThread().join();

        System.exit(0);
    }

    public static Task0<Integer> sum(int[] numbers)
    {
        return () -> {
            int sum = 0;
            for (int n : numbers)
                sum += n;

            return sum;
        };
    }
}