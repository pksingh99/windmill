package io.windmill.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import io.windmill.core.CPU;
import io.windmill.core.Future;

public class Futures
{
    private Futures()
    {}

    public static <T> T await(Future<T> future) throws Throwable
    {
        AtomicReference<T> value = new AtomicReference<>();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        future.onComplete(latch::countDown);

        Uninterruptibles.awaitUninterruptibly(latch);

        if (future.isFailure())
            throw exception.get();

        return value.get();
    }

    public static <T> Future<T> constantFuture(CPU cpu, T value)
    {
        return new ConstantFuture<>(cpu, value);
    }

    public static <T> Future<T> failedFuture(CPU cpu, Throwable e)
    {
        return new FailedFuture<>(cpu, e);
    }

    private static class FailedFuture<T> extends Future<T>
    {
        public FailedFuture(CPU cpu, Throwable e)
        {
            super(cpu);
            setFailure(e);
        }
    }

    private static class ConstantFuture<T> extends Future<T>
    {
        public ConstantFuture(CPU cpu, T value)
        {
            super(cpu);
            setValue(value);
        }
    }
}
