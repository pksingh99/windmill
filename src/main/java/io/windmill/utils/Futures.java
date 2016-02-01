package io.windmill.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

        future.onSuccess(value::set);
        future.onFailure(exception::set);
        future.onComplete(latch::countDown);

        awaitUninterruptibly(latch);

        if (future.isFailure())
            throw exception.get();

        return value.get();
    }

    public static Future<Void> voidFuture(CPU cpu)
    {
        return constantFuture(cpu, null);
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

    /* await/sleep methods where taken from Guava utilities and slightly modified */

    /**
     * Invokes {@code latch.}{@link CountDownLatch#await() await()} uninterruptibly.
     */
    public static void awaitUninterruptibly(CountDownLatch latch)
    {
        boolean interrupted = false;
        try
        {
            while (true)
            {
                try
                {
                    latch.await();
                    return;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
        }
        finally
        {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Invokes {@code latch.}{@link CountDownLatch#await(long, TimeUnit) await(timeout, unit)} uninterruptibly.
     */
    public static boolean awaitUninterruptibly(CountDownLatch latch, long timeout, TimeUnit unit)
    {
        boolean interrupted = false;
        try
        {
            long remainingNanos = unit.toNanos(timeout);
            long end = System.nanoTime() + remainingNanos;

            while (true)
            {
                try
                {
                    // CountDownLatch treats negative timeouts just like zero.
                    return latch.await(remainingNanos, TimeUnit.NANOSECONDS);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        }
        finally
        {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Invokes {@code unit.}{@link TimeUnit#sleep(long) sleep(sleepFor)} uninterruptibly.
     */
    public static void sleepUninterruptibly(long sleepFor, TimeUnit unit)
    {
        boolean interrupted = false;
        try
        {
            long remainingNanos = unit.toNanos(sleepFor);
            long end = System.nanoTime() + remainingNanos;
            while (true)
            {
                try
                {
                    // TimeUnit.sleep() treats negative timeouts just like zero.
                    TimeUnit.NANOSECONDS.sleep(remainingNanos);
                    return;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        }
        finally
        {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
