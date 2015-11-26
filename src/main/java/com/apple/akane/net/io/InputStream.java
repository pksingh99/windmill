package com.apple.akane.net.io;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import com.apple.akane.core.CPU;
import com.apple.akane.core.Future;
import com.apple.akane.net.TransferTask;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class InputStream implements AutoCloseable
{
    private final CPU cpu;
    private final SocketChannel channel;
    private final Queue<TransferTask<ByteBuf>> rxQueue;

    public InputStream(CPU cpu, SocketChannel channel)
    {
        this.cpu = cpu;
        this.channel = channel;
        this.rxQueue = new ArrayDeque<>();
    }

    public Future<ByteBuf> read(int size)
    {
        Future<ByteBuf> request = new Future<>(cpu);
        rxQueue.add(new RxTask(request, size));

        cpu.schedule(() -> {
            triggerRx();
            return null;
        });

        return request;
    }

    public void triggerRx()
    {
        while (!rxQueue.isEmpty())
        {
            TransferTask<ByteBuf> task = rxQueue.peek();

            if (!task.compute(channel))
                break;

            rxQueue.poll();
        }
    }

    @Override
    public void close()
    {
        while (!rxQueue.isEmpty())
            rxQueue.poll().close();
    }

    private static class RxTask extends TransferTask<ByteBuf>
    {
        public RxTask(Future<ByteBuf> request, int size)
        {
            super(Unpooled.buffer(size), request);
        }

        @Override
        public boolean compute(SocketChannel rx)
        {
            try
            {
                buffer.writeBytes(rx, buffer.writableBytes());
            }
            catch (IOException e)
            {
                onComplete.setFailure(e);
                return true;
            }

            if (buffer.writableBytes() > 0)
                return false;

            onComplete.setValue(buffer);
            return true;
        }
    }
}
