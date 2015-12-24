package com.apple.akane.net.io;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import com.apple.akane.core.CPU;
import com.apple.akane.core.Future;
import com.apple.akane.net.TransferTask;

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
        if (!rxQueue.rx(channel))
            return;

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

        public boolean rx(SocketChannel channel) throws IOException
        {
            // queue is full (read up to RX buffer size)
            if (availableBytes == maxSize)
                return false;

            ByteBuf component = Unpooled.buffer(512);
            int readBytes = component.writeBytes(channel, component.writableBytes());
            if (readBytes <= 0)
                return false;

            rx.add(component);
            availableBytes += readBytes;

            return true;
        }

        public ByteBuf transfer(int size)
        {
            CompositeByteBuf buffer = null;

            while (!rx.isEmpty())
            {
                ByteBuf rxBuffer = rx.peek();
                int position = rxBuffer.readerIndex();

                // perfect, we can satisfy I/O with a single buffer slice
                if (buffer == null && rxBuffer.readableBytes() >= size)
                {
                    ByteBuf slice = rxBuffer.slice(position, size);
                    // advance reader index of the original buffer
                    rxBuffer.readerIndex(position + size);
                    if (rxBuffer.readableBytes() == 0)
                        rx.poll(); // if completely consumed, remove from the list

                    availableBytes -= slice.readableBytes();
                    return slice;
                }

                if (buffer == null)
                    buffer = Unpooled.compositeBuffer();

                int consumableSize = Math.min(position, size);
                buffer.addComponent(rxBuffer.slice(position, consumableSize));

                // advance writer index of the composite buffer, to track read progress
                buffer.writerIndex(buffer.writerIndex() + consumableSize);
                // advance reader index of the original buffer
                rxBuffer.readerIndex(position + consumableSize);

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
    }
}
