package com.apple.akane.net.io;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import com.apple.akane.core.CPU;
import com.apple.akane.core.Future;
import com.apple.akane.net.TransferTask;

import io.netty.buffer.ByteBuf;

public class OutputStream implements AutoCloseable
{
    private final CPU cpu;
    private final SelectionKey key;
    private final SocketChannel channel;
    private final Queue<TransferTask<Integer>> txQueue;

    public OutputStream(CPU cpu, SelectionKey key, SocketChannel channel)
    {
        this.cpu = cpu;
        this.key = key;
        this.channel = channel;
        this.txQueue = new ArrayDeque<>();
    }

    public Future<Integer> writeAndFlush(ByteBuf buffer)
    {
        Future<Integer> future = new Future<>(cpu);

        if (!writeBytes(buffer, channel, future))
            return future; // return failed future right away

        if (buffer.readableBytes() == 0)
        {
            future.setValue(buffer.readableBytes());
            return future;
        }

        txQueue.add(new TxTask(buffer, future));
        key.interestOps(SelectionKey.OP_WRITE);

        return future;
    }

    public void triggerTx()
    {
        while (!txQueue.isEmpty())
        {
            TransferTask<Integer> task = txQueue.peek();

            if (!task.compute(channel))
                return;

            txQueue.poll();
        }

        // everything is flushed, switch back to read
        key.interestOps(SelectionKey.OP_READ);
    }

    @Override
    public void close()
    {
        while (!txQueue.isEmpty())
            txQueue.poll().close();
    }

    private static class TxTask extends TransferTask<Integer>
    {
        public TxTask(ByteBuf buffer, Future<Integer> future)
        {
            super(buffer, future);
        }

        @Override
        public boolean compute(SocketChannel channel)
        {
            // if write failed still report this as success since failure was set
            if (!writeBytes(buffer, channel, onComplete))
                return true;

            if (buffer.readableBytes() > 0)
                return false;

            onComplete.setValue(buffer.readableBytes());
            return true;
        }
    }

    private static boolean writeBytes(ByteBuf buffer, SocketChannel channel, Future<Integer> future)
    {
        try
        {
            buffer.readBytes(channel, buffer.readableBytes());
        }
        catch (IOException e)
        {
            future.setFailure(e);
            return false;
        }

        return true;
    }
}
