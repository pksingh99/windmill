package com.apple.akane.core;

import java.io.IOException;
import java.net.InetSocketAddress;

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

public class CPU implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CPU.class);

    private static final EventPoller.Handler<WorkEvent> HANDLER = (event, sequence, endOfBatch) -> {
        event.run();
        return false;
    };

    private volatile boolean isHalted = false;

    protected final RingBuffer<WorkEvent> runQueue;
    protected final Network network;

    public CPU() throws IOException
    {
        runQueue = RingBuffer.create(ProducerType.MULTI, WorkEvent::new, 1 << 20, new BusySpinWaitStrategy());
        network = new Network(this);
    }

    public void listen(InetSocketAddress address, VoidTask<Channel> onAccept) throws IOException
    {
        network.listen(address, onAccept);
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

    @Override
    public void run()
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
