package io.windmill.net.io;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.net.TransferTask;
import io.windmill.utils.Futures;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class InputStream implements AutoCloseable
{
    private final CPU cpu;
    private final SocketChannel channel;

    private final RxQueue rxQueue;
    private final Queue<RxTask> pendingTasks;

    public InputStream(CPU cpu, SocketChannel channel) throws SocketException
    {
        this.cpu = cpu;
        this.channel = channel;
        this.pendingTasks = new ArrayDeque<>();
        this.rxQueue = new RxQueue(channel.socket().getReceiveBufferSize());
    }

    public Future<ByteBuf> read(int size)
    {
        if (!channel.isOpen())
            return Futures.failedFuture(cpu, new ClosedChannelException());

        Future<ByteBuf> ioPromise = new Future<>(cpu);

        if (rxQueue.availableBytes() < size || !pendingTasks.isEmpty())
        {
            // schedule I/O, since not enough bytes are available yet or there are pending requests
            pendingTasks.add(new RxTask(ioPromise, size));
        }
        else
        {
            // no pending tasks and enough data available, can satisfy request inline
            ioPromise.setValue(rxQueue.transfer(size));
        }

        return ioPromise;
    }

    public void triggerRx() throws IOException
    {
        rxQueue.rx(channel);
        triggerTasks();
    }

    private void triggerTasks()
    {
        while (!pendingTasks.isEmpty())
        {
            RxTask task = pendingTasks.peek();
            if (!task.compute(rxQueue))
                break;

            pendingTasks.poll();
        }
    }

    @Override
    public void close()
    {
        // run pending tasks for the last
        // time to consume as much as possible from the rx buffer
        triggerTasks();

        rxQueue.close();
        while (!pendingTasks.isEmpty())
            pendingTasks.poll().close();
    }

    private static class RxTask extends TransferTask<RxQueue, ByteBuf>
    {
        private final int size;

        public RxTask(Future<ByteBuf> request, int size)
        {
            super(null, request);
            this.size = size;
        }

        @Override
        public boolean compute(RxQueue rx)
        {
            if (rx.availableBytes() < size)
                return false;

            try
            {
                onComplete.setValue(rx.transfer(size));
            }
            catch (Exception | Error e)
            {
                onComplete.setFailure(e);
            }

            return true;
        }
    }

    private static class RxQueue implements AutoCloseable
    {
        private final Queue<ByteBuf> rx;
        private final int maxSize;
        private int availableBytes;

        public RxQueue(int maxSize)
        {
            this.rx = new ArrayDeque<>();
            this.maxSize = maxSize;
        }

        public void rx(SocketChannel channel) throws IOException
        {
            // queue is full (read up to RX buffer size)
            if (availableBytes == maxSize)
                return;

            // first let's try to re-use already existing top "in-progress" buffer
            ByteBuf inProgress = rx.peek();
            if (inProgress != null && inProgress.writableBytes() > 0)
            {
                int readBytes = inProgress.writeBytes(channel, inProgress.writableBytes());
                if (readBytes <= 0) // nothing has been read
                    return;

                availableBytes += readBytes;
                // channel didn't have enough readable bytes to fill up "in-progress" buffer
                if (inProgress.writableBytes() > 0)
                    return;
            }

            // so something might be left in the buffer, let's try to allocate new component
            // and read the remainder if any
            ByteBuf component = Unpooled.buffer(512);
            availableBytes += component.writeBytes(channel, component.writableBytes());
            rx.add(component);
        }

        public ByteBuf transfer(int size)
        {
            CompositeByteBuf buffer = null;

            while (!rx.isEmpty())
            {
                ByteBuf rxBuffer = rx.peek();

                // perfect, we can satisfy I/O with a single buffer slice
                if (buffer == null && rxBuffer.readableBytes() >= size)
                {
                    ByteBuf slice = consume(rxBuffer, size);
                    if (rxBuffer.readableBytes() == 0)
                        rx.poll(); // if completely consumed, remove from the list

                    availableBytes -= slice.readableBytes();
                    return slice;
                }

                if (buffer == null)
                    buffer = Unpooled.compositeBuffer();

                int consumableSize = Math.min(rxBuffer.readableBytes(), size);
                buffer.addComponent(consume(rxBuffer, consumableSize));

                // advance writer index of the composite buffer, to track read progress
                buffer.writerIndex(buffer.writerIndex() + consumableSize);

                size -= consumableSize;
                availableBytes -= consumableSize;

                if (rxBuffer.readableBytes() == 0)
                    rx.poll();

                if (size == 0)
                    break;
            }

            return buffer;
        }

        public int availableBytes()
        {
            return availableBytes;
        }

        @Override
        public void close()
        {
            while (!rx.isEmpty())
                rx.poll().release();

            availableBytes = 0;
        }

        private ByteBuf consume(ByteBuf src, int length)
        {
            int position = src.readerIndex();
            ByteBuf slice = src.slice(position, length);
            src.readerIndex(position + length);
            return slice;
        }
    }
}
