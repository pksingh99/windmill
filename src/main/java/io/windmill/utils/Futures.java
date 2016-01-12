package io.windmill.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
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

        future.onFailure((e) -> {
            try
            {
                exception.set(e);
            }
            finally
            {
                latch.countDown();
            }
        });

        future.onSuccess((v) -> {
            try
            {
                value.set(v);
            }
            finally
            {
                latch.countDown();
            }
        });

        Uninterruptibles.awaitUninterruptibly(latch);

        if (future.isFailure())
            throw exception.get();

        return value.get();
    }
}
