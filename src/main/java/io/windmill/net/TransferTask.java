package io.windmill.net;

import java.nio.channels.ClosedChannelException;
import java.util.Optional;

import io.windmill.core.Future;

import io.netty.buffer.ByteBuf;

public abstract class TransferTask<I, O> implements AutoCloseable
{
    protected final ByteBuf buffer;
    protected final Optional<Future<O>> onComplete;

    public TransferTask(ByteBuf buffer, Optional<Future<O>> onComplete)
    {
        this.buffer = buffer;
        this.onComplete = onComplete;
    }

    public abstract boolean compute(I channel);

    public Future<O> getFuture()
    {
        return onComplete.isPresent() ? onComplete.get() : null;
    }

    @Override
    public void close()
    {
        buffer.release();
        onComplete.ifPresent((f) -> f.setFailure(new ClosedChannelException()));
    }
}
