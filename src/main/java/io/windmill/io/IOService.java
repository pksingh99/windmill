package io.windmill.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import net.openhft.affinity.AffinitySupport;

public class IOService implements AutoCloseable
{
    private final CPU cpu;
    private final ExecutorService io;

    public IOService(CPU cpu, int numThreads)
    {
        this.cpu = cpu;
        this.io = Executors.newFixedThreadPool(numThreads, new LayoutAwareThreadFactory(cpu));
    }

    public Future<File> open(String path, String mode)
    {
        return schedule(() -> new File(this, new RandomAccessFile(path, mode)));
    }

    <O> Future<O> schedule(IOTask<O> task)
    {
        Future<O> future = new Future<>(cpu);
        io.execute(() -> {
            try
            {
                O value = task.compute();
                cpu.schedule(() -> {
                    future.setValue(value);
                    return null;
                });
            }
            catch (IOException e)
            {
                cpu.schedule(() -> {
                    future.setFailure(e);
                    return null;
                });
            }
        });
        return future;
    }

    @Override
    public void close() throws Exception
    {
        io.shutdown();
    }

    private static class LayoutAwareThreadFactory implements ThreadFactory
    {
        private final CPU cpu;
        private final AtomicInteger threadId;

        public LayoutAwareThreadFactory(CPU cpu)
        {
            this.cpu = cpu;
            this.threadId = new AtomicInteger(0);
        }

        @Override
        public Thread newThread(Runnable task)
        {
            Thread newThread = new Thread(() -> {
                if (cpu.getLayout() != null)
                    AffinitySupport.setAffinity(1L << cpu.getId());

                task.run();
            }, cpu.getId() + "-io:" + threadId.incrementAndGet());

            newThread.setDaemon(true);
            return newThread;
        }
    }
}
