package com.apple.akane.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;

import com.apple.akane.core.tasks.Task0;
import com.apple.akane.core.tasks.Task1;
import com.apple.akane.core.tasks.VoidTask;
import com.apple.akane.net.Channel;
import com.apple.akane.net.Network;
import com.apple.akane.utils.IOUtils;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.EventPoller.PollState;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPU
{
    private static final Logger logger = LoggerFactory.getLogger(CPU.class);

    private static final EventPoller.Handler<WorkEvent> HANDLER = (event, sequence, endOfBatch) -> {
        event.run();
        return false;
    };

    private volatile boolean isHalted = false;

    protected final int id;
    protected final CPUSet.Socket socket;
    protected final RingBuffer<WorkEvent> runQueue;
    protected final Network network;

    CPU(int cpuId, CPUSet.Socket socket)
    {
        id = cpuId;
        this.socket = socket;
        runQueue = RingBuffer.create(ProducerType.MULTI, WorkEvent::new, 1 << 20, new BusySpinWaitStrategy());
        network = new Network(this);
    }

    public void listen(InetSocketAddress address, VoidTask<Channel> onAccept, VoidTask<Throwable> onFailure) throws IOException
    {
        network.listen(address, onAccept, onFailure);
    }

    public <O> Future<O> schedule(Task0<O> task)
    {
        return schedule(new Promise<>(this, task));
    }

    public <O> void loop(Task1<CPU, Future<O>> task)
    {
        schedule(() -> task.compute(this).onSuccess((o) -> loop(task)));
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
        new Thread(this::run).start();
    }

    protected void run()
    {
        EventPoller<WorkEvent> poller = runQueue.newPoller();

        while (!isHalted)
        {
            try
            {
                if (poller.poll(HANDLER) != PollState.PROCESSING)
                    network.poll();
            }
            catch (Exception e)
            {
                logger.error("task failed", e);
            }
        }

        IOUtils.closeQuietly(network);
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
}
