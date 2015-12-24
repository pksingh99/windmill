package com.apple.akane.net;

import java.nio.channels.ClosedChannelException;

import com.apple.akane.core.Future;

import io.netty.buffer.ByteBuf;

public abstract class TransferTask<I, O> implements AutoCloseable
{
    protected final ByteBuf buffer;
    protected final Future<O> onComplete;

    public TransferTask(ByteBuf buffer, Future<O> onComplete)
    {
        this.buffer = buffer;
        this.onComplete = onComplete;
    }

    public abstract boolean compute(I channel);

    @Override
    public void close()
    {
        buffer.release();
        onComplete.setFailure(new ClosedChannelException());
    }
}
