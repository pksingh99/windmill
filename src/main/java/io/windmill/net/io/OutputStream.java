package io.windmill.net.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.net.TransferTask;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class OutputStream implements AutoCloseable
{
    private final CPU cpu;
    private final SelectionKey key;
    private final SocketChannel channel;
    private final Queue<TransferTask<SocketChannel, ?>> txQueue;

    public OutputStream(CPU cpu, SelectionKey key, SocketChannel channel)
    {
        this.cpu = cpu;
        this.key = key;
        this.channel = channel;
        this.txQueue = new ArrayDeque<>();
    }

    public OutputStream writeInt(int v)
    {
        writeBytes(Unpooled.copyInt(v));
        return this;
    }

    public OutputStream writeBytes(ByteBuf bytes)
    {
        txQueue.add(new TxTask(bytes, Optional.empty()));
        return this;
    }

    public Future<Void> flush()
    {
        return writeAndFlush(Unpooled.EMPTY_BUFFER).map((v) -> null);
    }

    public Future<Long> writeAndFlush(ByteBuf buffer)
    {
        return writeAndFlush(new TxTask(buffer, Optional.of(new Future<>(cpu))));
    }

    public Future<Long> transferFrom(FileChannel channel, long offset, long length)
    {
        return writeAndFlush(new FileTxTask(channel, offset, length, new Future<>(cpu)));
    }

    public <T> Future<T> writeAndFlush(TransferTask<SocketChannel, T> task)
    {
        if (!channel.isOpen())
             task.close();

        if (txQueue.size() == 0 && task.compute(channel))
            return task.getFuture();

        txQueue.add(task);
        key.interestOps(SelectionKey.OP_WRITE);

        return task.getFuture();
    }

    public void triggerTx()
    {
        while (!txQueue.isEmpty())
        {
            TransferTask<SocketChannel, ?> task = txQueue.peek();
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

    private static class TxTask extends TransferTask<SocketChannel, Long>
    {
        public TxTask(ByteBuf buffer, Optional<Future<Long>> future)
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

            onComplete.ifPresent((f) -> f.setValue((long) buffer.readableBytes()));
            return true;
        }
    }

    private static class FileTxTask extends TransferTask<SocketChannel, Long>
    {
        private final FileChannel file;
        private long offset, remaining;
        private long transferred = 0;

        public FileTxTask(FileChannel file, long offset, long length, Future<Long> future)
        {
            super(null, Optional.of(future));

            this.file = file;
            this.offset = offset;
            this.remaining = length;
        }

        @Override
        public boolean compute(SocketChannel socket)
        {
            try
            {
                long len = file.transferTo(offset, remaining, socket);

                offset += len;
                remaining -= len;
                transferred += len;

                // haven't reached the end of the file yet
                if (remaining > 0 && offset < file.size())
                    return false;

                onComplete.ifPresent((f) -> f.setValue(transferred));
            }
            catch (IOException e)
            {
                onComplete.ifPresent((f) -> f.setFailure(e));
            }

            return true;
        }
    }

    private static boolean writeBytes(ByteBuf buffer, SocketChannel channel, Optional<Future<Long>> future)
    {
        try
        {
            buffer.readBytes(channel, buffer.readableBytes());
        }
        catch (IOException e)
        {
            future.ifPresent((f) -> f.setFailure(e));
            return false;
        }

        return true;
    }
}
