package io.windmill.disk;

import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.windmill.core.CPU;
import io.windmill.core.Future;

import net.openhft.affinity.AffinitySupport;

import com.github.benmanes.caffeine.cache.Cache;

public class IOService implements AutoCloseable
{
    protected final CPU cpu;
    protected final ExecutorService io;
    protected final Cache<PageRef, Boolean> pageTracker;

    public IOService(CPU cpu, Cache<PageRef, Boolean> pageTracker, int numThreads)
    {
        this.cpu = cpu;
        this.io = Executors.newFixedThreadPool(numThreads, new LayoutAwareThreadFactory(cpu));
        this.pageTracker = pageTracker;
    }

    public Future<File> open(String path, String mode)
    {
        return schedule(() -> new File(cpu, this, new RandomAccessFile(path, mode)));
    }

    public CPU getCPU()
    {
        return cpu;
    }

    public <O> Future<O> schedule(IOTask<O> task)
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
            catch (Throwable e)
            {
                cpu.schedule(() -> {
                    future.setFailure(e);
                    return null;
                });
            }
        });
        return future;
    }

    void markPageAccessed(File file, int pageOffset)
    {
        pageTracker.put(new PageRef(file, pageOffset), true);
    }

    void markPageEvicted(File file, int pageOffset)
    {
        pageTracker.invalidate(new PageRef(file, pageOffset));
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
                cpu.setAffinity();

                task.run();
            }, cpu.getId() + "-io:" + threadId.incrementAndGet());

            newThread.setDaemon(true);
            return newThread;
        }
    }

}
