package io.windmill.core;

import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.windmill.core.tasks.*;
import io.windmill.core.Status.Flag;
import io.windmill.disk.File;
import io.windmill.disk.IOService;
import io.windmill.disk.IOTask;
import io.windmill.disk.PageRef;
import io.windmill.net.Channel;
import io.windmill.net.Network;
import io.windmill.utils.IOUtils;

import com.github.benmanes.caffeine.cache.Cache;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.EventPoller.PollState;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.ProducerType;

import net.openhft.affinity.AffinitySupport;
import net.openhft.affinity.CpuLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Windmill's primary execution abstraction: the per-core run loop,
 * network selector, and io scheduler.
 *
 * All work in windmill is scheduled on a {@link CPU}, using any of the
 * functions defined on it, such as {@link #schedule(Task0)}, {@link #open(java.io.File, String)},
 * {@link #connect(InetSocketAddress)}, {@link #listen(InetSocketAddress, VoidTask1, VoidTask1)},
 * or {@link #repeat(Task1)} functions, or by performing operations on the {@link Future<0>}s
 * returned by these operations.
 *
 * The number of IO threads used by each windmill CPU is configurable via the
 * {@code windmill.cpu.io_threads} system property.
 */
public class CPU
{
    private static final Logger logger = LoggerFactory.getLogger(CPU.class);

    // default number of I/O threads per CPU
    private static final int DEFAULT_IO_THREADS = Integer.getInteger("windmill.cpu.io_threads", 4);

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

    CPU(CpuLayout layout, int cpuId, CPUSet.Socket socket, Cache<PageRef, Boolean> pageTracker)
    {
        this.layout = layout;
        this.id = cpuId;
        this.socket = socket;
        this.runQueue = RingBuffer.create(ProducerType.MULTI, WorkEvent::new, 1 << 20, new BusySpinWaitStrategy());
        this.io = new IOService(this, pageTracker, DEFAULT_IO_THREADS);
        this.network = new Network(this);
        this.timers = new DelayQueue<>();
    }

    /**
     * @return the id for this CPU
     */
    public int getId()
    {
        return id;
    }

    /**
     * @return the {@link CpuLayout} this {@link CPU} belongs to
     */
    public CpuLayout getLayout()
    {
        return layout;
    }

    /**
     * Bind a socket to a given {@link InetSocketAddress} and listen for incoming connections
     *
     * @param address the address to listen for connections on
     * @param onAccept executed when each new connection is accepted, the {@link Channel}
     *                 object can be used to perform operations on the accepted connection
     * @param onFailure executed if there was an exception while binding to the given address
     */
    public void listen(InetSocketAddress address, VoidTask1<Channel> onAccept, VoidTask1<Throwable> onFailure)
    {
        network.listen(address, onAccept, onFailure);
    }

    /**
     * Try to make a connection to the given {@link InetSocketAddress}.
     *
     * @param address the address to connect to
     * @return a {@link Future<Channel>} that represents the eventual connection or failure,
     * the {@link Channel} object can be used to perform further operations on the conncetion.
     */
    public Future<Channel> connect(InetSocketAddress address)
    {
        return network.connect(address);
    }

    /**
     * Schedule arbitrary work, which does not return a value, on this CPU
     *
     * @param task the work to execute
     * @return a {@link Future<Void>} that can be used to interact with the completion of the work or
     * any exceptions that occur during its execution.
     */
    public Future<Void> schedule(VoidTask0 task)
    {
        return schedule(() -> { task.compute(); return null; });
    }

    /**
     * Schedule arbitrary work on this CPU
     *
     * @param task the work to execute
     * @param <O> the type of value the work returns
     * @return a {@link Future<O>} that can be used to schedule more work based on the result, or to handle
     * any exceptions that occurred during execution.
     */
    public <O> Future<O> schedule(Task0<O> task)
    {
        return schedule(new Promise<>(this, task));
    }

    /**
     * Schedule work on one of the IO threads managed by this CPU. This is intended for work that
     * performs IO, enabling asynchronous IO
     *
     * @param task the work to execute
     * @param <O> the type of value the work returns
     * @return a {@link Future<O>} that can be used to schedule more work based on the result, or to handle
     * any exceptions that occurred during execution.
     */
    public <O> Future<O> scheduleIO(IOTask<O> task)
    {
        return io.schedule(task);
    }

    /**
     * Perform a side-effecting task continuously - executing the next iteration once the previous has completed. Tasks
     * can indicate whether or not to continue via the {@link Status} in the returned future.
     *
     * @param task the work to perform repeatedly
     */
    public void repeat(Task1<CPU, Future<Status<Void>>> task)
    {
        repeat((Task2<CPU, Void, Future<Status<Void>>>) (cpu, previous) -> task.compute(cpu));
    }

    /**
     * Perform a task continuously - executing the next iteration once the previous has completed. Tasks
     * can indicate whether or not to continue, and propogate the previous result, via the {@link Status<O>} in the returned future.
     *
     * @param task the work to perform repeatedly
     * @param <O> the type of value the work produces
     */
    public <O> void repeat(Task2<CPU, O, Future<Status<O>>> task)
    {
        repeat(task, null);
    }

    private <O> void repeat(Task2<CPU, O, Future<Status<O>>> task, O previous)
    {
        schedule(() -> task.compute(this, previous).onSuccess((status) -> {
            if (status.flag == Flag.CONTINUE)
                repeat(task, status.value);
        }));
    }

    /**
     * Given a list of asynchronous work, all returning the same type of value, {@code I},
     * return a single {@link Future<I>} representing the successful completion of all of
     * the work, or one or more failures
     *
     * @param futures the list of work to sequence
     * @param <I> the type of value returned by every task in the list
     * @return a {@link Future<I>} representing the successful completion of all of
     * the work, or one or more failures
     */
    public <I> Future<List<I>> sequence(List<Future<I>> futures)
    {
        Future<List<I>> promise = new Future<>(this);
        schedule(() -> {
            AtomicInteger counter = new AtomicInteger(0);
            List<I> results = new ArrayList<>(futures.size());

            for (int i = 0; i < futures.size(); i++)
            {
                int currentIndex = i;
                Future<I> f = futures.get(i);

                // pre-allocate the space for the elements being added out of order
                results.add(null);

                // both success and failure handling
                // should be done based on the correct CPU context
                f.onSuccess((v) -> schedule(() -> {
                    results.set(currentIndex, v);

                    // collect all of the results before
                    // marking flattened future as success
                    if (counter.incrementAndGet() >= futures.size())
                        promise.setValue(results);
                }));

                // if at least one of the futures fails, fail flattened promise.
                f.onFailure((e) -> schedule(() -> promise.setFailure(e)));
            }

        });

        return promise;
    }

    /**
     * Perform arbitrary work after a given delay
     *
     * @param duration the amount to delay for
     * @param unit the unit of {@code duration}
     * @param then the work to execute after the given delay
     * @param <O> the type of value the work returns
     * @return a {@link Future<O>} allow work to be scheduled based on the result
     * of the work executing after the given delay
     */
    public <O> Future<O> sleep(long duration, TimeUnit unit, Task0<O> then)
    {
        Promise<O> promise = new Promise<>(this, then);
        timers.add(new TimerTask<>(unit.toNanos(duration), promise));
        return promise.getFuture();
    }

    /**
     * Open a file asynchronously
     *
     * @param path the absolute or relative path to the file
     * @param mode see {@link java.io.RandomAccessFile} for a description of this argument
     * @return a {@link Future<File>} that can be used to perform operations on the file,
     * if successfully opened, or handle any exceptions that may have occurred
     */
    public Future<File> open(String path, String mode)
    {
        return io.open(path, mode);
    }

    /**
     * Open a file asynchronously
     *
     * @param file the file to open
     * @param mode see {@link java.io.RandomAccessFile} for a description of this argument
     * @return a {@link Future<File>} that can be used to perform operations on the file,
     * if successfully opened, or handle any exceptions that may have occurred
     */
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

    /**
     * @return the {@link io.windmill.core.CPUSet.Socket} this {@link CPU} belongs to
     */
    public CPUSet.Socket getSocket()
    {
        return socket;
    }

    protected Selector getSelector()
    {
        return network.getSelector();
    }

    /**
     * Starts the thread that this CPU runs on. This must be called before any work
     * is scheduled on this CPU.
     */
    public void start()
    {
        Thread thread = new Thread(this::run);
        thread.setName(id + "-app");
        thread.start();
    }

    protected void run()
    {
        setAffinity();

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

    /**
     * Eventually stop the thread that is executing work scheduled on this {@link CPU}.
     * This stops any future work from being performed by this {@link CPU}.
     */
    public void halt()
    {
        isHalted = true;
    }

    public void setAffinity()
    {
        try
        {
            if (layout == null)
                AffinitySupport.setAffinity(1L << id);
        }
        catch (IllegalStateException e)
        {
            logger.warn("failed to set affinity for CPU {}, ignoring...", id, e);
        }
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
