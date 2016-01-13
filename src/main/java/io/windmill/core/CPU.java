package io.windmill.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import io.windmill.core.tasks.Task0;
import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.VoidTask1;
import io.windmill.io.File;
import io.windmill.io.IOService;
import io.windmill.net.Channel;
import io.windmill.net.Network;
import io.windmill.utils.IOUtils;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.EventPoller.PollState;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.ProducerType;

import net.openhft.affinity.AffinitySupport;
import net.openhft.affinity.CpuLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPU
{
    private static final Logger logger = LoggerFactory.getLogger(CPU.class);

    // default number of I/O threads per CPU
    private static final int DEFAULT_IO_THREADS = 4;

    private static final EventPoller.Handler<WorkEvent> HANDLER = (event, sequence, endOfBatch) -> {
        event.run();
        return false;
    };

    private volatile boolean isHalted = false;

    protected final CpuLayout layout;
    protected final int id;
    protected final CPUSet.Socket socket;
    protected final RingBuffer<WorkEvent> runQueue;
    protected final IOService io;
    protected final Network network;
    protected final DelayQueue<TimerTask> timers;

    CPU(CpuLayout layout, int cpuId, CPUSet.Socket socket)
    {
        this.layout = layout;
        this.id = cpuId;
        this.socket = socket;
        this.runQueue = RingBuffer.create(ProducerType.MULTI, WorkEvent::new, 1 << 20, new BusySpinWaitStrategy());
        this.io = new IOService(this, DEFAULT_IO_THREADS);
        this.network = new Network(this);
        this.timers = new DelayQueue<>();
    }

    public int getId()
    {
        return id;
    }

    public CpuLayout getLayout()
    {
        return layout;
    }

    public void listen(InetSocketAddress address, VoidTask1<Channel> onAccept, VoidTask1<Throwable> onFailure) throws IOException
    {
        network.listen(address, onAccept, onFailure);
    }

    public <O> Future<O> schedule(Task0<O> task)
    {
        return schedule(new Promise<>(this, task));
    }

    public <O> void loop(Task1<CPU, Future<O>> task)
    {
        schedule(() -> task.compute(this).map((o) -> {
            loop(task);
            return null;
        }));
    }

    public <O> Future<O> sleep(long duration, TimeUnit unit, Task0<O> then)
    {
        Promise<O> promise = new Promise<>(this, then);
        timers.add(new TimerTask<>(unit.toNanos(duration), promise));
        return promise.getFuture();
    }

    public Future<File> open(String path, String mode)
    {
        return io.open(path, mode);
    }

    public Future<File> open(java.io.File file, String mode)
    {
        return io.open(file.getAbsolutePath(), mode);
    }

    protected <O> Future<O> schedule(Promise<O> promise)
    {
        long sequence = runQueue.next();

        try
        {
            WorkEvent event = runQueue.get(sequence);
            event.setWork(promise);
        }
        finally
        {
            runQueue.publish(sequence);
        }

        return promise.getFuture();
    }

    public CPUSet.Socket getSocket()
    {
        return socket;
    }

    protected Selector getSelector()
    {
        return network.getSelector();
    }

    public void start()
    {
        Thread thread = new Thread(this::run);
        thread.setName(id + "-app");
        thread.start();
    }

    protected void run()
    {
        if (layout != null)
            AffinitySupport.setAffinity(1L << id);

        EventPoller<WorkEvent> poller = runQueue.newPoller();

        while (!isHalted)
        {
            try
            {
                if (poller.poll(HANDLER) != PollState.PROCESSING)
                    network.poll();

                processTimers();
            }
            catch (Exception e)
            {
                logger.error("task failed", e);
            }
        }

        IOUtils.closeQuietly(io);
        IOUtils.closeQuietly(network);
    }

    protected void processTimers()
    {
        for (;;)
        {
            TimerTask<?> task = timers.poll();
            if (task == null)
                break;

            schedule(task.promise);
        }
    }

    public void halt()
    {
        isHalted = true;
    }

    private class WorkEvent implements Runnable
    {
        private Promise promise;

        public void setWork(Promise promise)
        {
            this.promise = promise;
        }

        public void run()
        {
            promise.fulfil();
            promise = null; // release a reference to already processed promise
        }
    }

    private static class TimerTask<O> implements Delayed
    {
        private final long startTime;
        private final Promise<O> promise;

        public TimerTask(long delayNanos, Promise<O> promise)
        {
            this.startTime = System.nanoTime() + delayNanos;
            this.promise = promise;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(startTime - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other)
        {
            if (other == null || !(other instanceof TimerTask))
                return -1;

            if (other == this)
                return 0;

            return Long.compare(startTime, ((TimerTask) other).startTime);
        }
    }
}
