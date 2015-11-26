package com.apple.akane.net;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import com.apple.akane.core.Future;

import io.netty.buffer.ByteBuf;

public abstract class TransferTask<T> implements AutoCloseable
{
    protected final ByteBuf buffer;
    protected final Future<T> onComplete;

    public TransferTask(ByteBuf buffer, Future<T> onComplete)
    {
        this.buffer = buffer;
        this.onComplete = onComplete;
    }

    public abstract boolean compute(SocketChannel channel);

    @Override
    public void close()
    {
        buffer.release();
        onComplete.setFailure(new ClosedChannelException());
    }
}
