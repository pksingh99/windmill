package com.apple.akane.core;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

public class CPUTest extends AbstractTest
{
    @Test
    public void testLoop()
    {
        CountDownLatch latch = new CountDownLatch(10);

        CPU.loop((cpu) -> {
            try
            {
                return (latch.getCount() == 0)
                        ? new Future<Integer>(cpu) {{ setFailure(null); }}
                        : new ConstantFuture<>(cpu, 42);
            }
            finally
            {
                latch.countDown();
            }
        });

        Uninterruptibles.awaitUninterruptibly(latch);
    }
}
