package io.windmill.core;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.windmill.core.tasks.Task1;
import io.windmill.utils.Futures;

import org.junit.Assert;
import org.junit.Test;

public class FutureTest extends AbstractTest
{
    @Test(expected = IllegalStateException.class)
    public void testGetOnUnresolvedFuture()
    {
        new Future<>(CPUs.get(0)).get();
    }

    @Test
    public void testSuccess()
    {
        Future<Integer> future = new Future<>(null);
        future.setValue(42);

        Assert.assertTrue(future.isAvailable());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(42, (int) future.get());
    }

    @Test
    public void testFailure()
    {
        IllegalArgumentException e = new IllegalArgumentException();

        Future<Integer> future = new Future<>(null);
        future.setFailure(e);

        Assert.assertTrue(future.isAvailable());
        Assert.assertFalse(future.isSuccess());
        Assert.assertTrue(future.isFailure());

        Assert.assertEquals(null, future.get());
        Assert.assertNotNull(future.onFailure);
        Assert.assertEquals(e, future.onFailure.get());
    }

    @Test
    public void testMap()
    {
        CPU cpu = CPUs.get(0);

        Future<Integer> futureA = new Future<>(cpu);

        AtomicInteger result = new AtomicInteger(0);

        CountDownLatch latchA = new CountDownLatch(1);

        futureA.map((n) -> mapTask(result, latchA).compute(n));

        setValue(cpu, futureA, 42);

        Futures.awaitUninterruptibly(latchA);
        Assert.assertEquals(42, result.get());

        result.set(0);

        CountDownLatch latchB = new CountDownLatch(2);

        // multiple map tasks with already complete future
        futureA.map((n) -> mapTask(result, latchB).compute(n));
        futureA.map((n) -> mapTask(result, latchB).compute(n));

        Futures.awaitUninterruptibly(latchB);
        Assert.assertEquals(84, result.get());

        result.set(0);

        CountDownLatch latchC = new CountDownLatch(2);
        Future<Integer> futureB = new Future<>(cpu);

        // multiple map tasks with unfulfilled future
        futureB.map((n) -> mapTask(result, latchC).compute(n));
        futureB.map((n) -> mapTask(result, latchC).compute(n));

        setValue(cpu, futureB, 1);

        Futures.awaitUninterruptibly(latchC);
        Assert.assertEquals(2, result.get());
    }

    @Test
    public void testFlatMap()
    {
        CPU cpu = CPUs.get(0);

        Future<Future<Integer>> futureA = new Future<>(cpu);
        CountDownLatch latchA = new CountDownLatch(1);

        AtomicInteger result = new AtomicInteger(0);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        futureA.flatMap((o) -> {
            o.onSuccess((n) -> mapTask(result, latchA).compute(n));
            o.onFailure((e) -> {
                Assert.fail(); // is going to be logged on error, shouldn't happen
            });
            return null;
        });

        setValue(cpu, futureA, new Future<Integer>(cpu) {{
            setValue(42);
        }});

        Futures.awaitUninterruptibly(latchA);
        Assert.assertEquals(42, result.get());

        Future<Future<Integer>> futureB = new Future<>(cpu);
        CountDownLatch latchB = new CountDownLatch(1);

        futureB.flatMap((o) -> {
            o.onSuccess((n) -> Assert.fail());
            o.onFailure((e) -> exceptionTask(failure, latchB).compute(e));

            return null;
        });

        RuntimeException ex = new RuntimeException();
        setValue(cpu, futureB, new Future<Integer>(cpu) {{ setFailure(ex); }});

        Futures.awaitUninterruptibly(latchB);
        Assert.assertEquals(ex, failure.get());
    }

    @Test
    public void testOnSuccess()
    {
        CPU cpu = CPUs.get(0);

        Future<Integer> futureA = new Future<>(cpu);

        AtomicInteger result = new AtomicInteger(0);

        CountDownLatch latchA = new CountDownLatch(1);

        futureA.onSuccess((n) -> mapTask(result, latchA).compute(n));

        setValue(cpu, futureA, 42);

        Futures.awaitUninterruptibly(latchA);
        Assert.assertEquals(42, result.get());

        result.set(0);

        CountDownLatch latchB = new CountDownLatch(2);

        // multiple map tasks with already complete future
        futureA.onSuccess((n) -> mapTask(result, latchB).compute(n));
        futureA.onSuccess((n) -> mapTask(result, latchB).compute(n));

        Futures.awaitUninterruptibly(latchB);
        Assert.assertEquals(84, result.get());

        result.set(0);

        CountDownLatch latchC = new CountDownLatch(2);
        Future<Integer> futureB = new Future<>(cpu);

        // multiple map tasks with unfulfilled future
        futureB.onSuccess((n) -> mapTask(result, latchC).compute(n));
        futureB.onSuccess((n) -> mapTask(result, latchC).compute(n));

        setValue(cpu, futureB, 1);

        Futures.awaitUninterruptibly(latchC);
        Assert.assertEquals(2, result.get());
    }

    @Test
    public void testOnFailure()
    {
        CPU cpu = CPUs.get(0);

        IllegalArgumentException ex = new IllegalArgumentException();
        AtomicReference<Throwable> exception = new AtomicReference<>();

        Future<Integer> failedFuture = new Future<>(cpu);
        CountDownLatch latchA = new CountDownLatch(1);

        failedFuture.onFailure((e) -> exceptionTask(exception, latchA).compute(e));

        setFailure(cpu, failedFuture, ex);

        Futures.awaitUninterruptibly(latchA);

        Assert.assertTrue(failedFuture.isAvailable());
        Assert.assertTrue(failedFuture.isFailure());
        Assert.assertEquals(ex, exception.get());

        exception.set(null);
        Assert.assertNull(exception.get());

        // on failure set on the already failed future
        CountDownLatch latchB = new CountDownLatch(1);
        failedFuture.onFailure((e) -> exceptionTask(exception, latchB).compute(e));

        Futures.awaitUninterruptibly(latchB);

        Assert.assertTrue(failedFuture.isAvailable());
        Assert.assertTrue(failedFuture.isFailure());
        Assert.assertEquals(ex, exception.get());

        AtomicInteger universalNumber = new AtomicInteger(0);
        CountDownLatch latchC = new CountDownLatch(1);
        failedFuture.onFailure((e) -> mapTask(universalNumber, latchC).compute(42));

        Futures.awaitUninterruptibly(latchC);
        Assert.assertEquals(42, universalNumber.get());

        universalNumber.set(0);
        CountDownLatch latchD = new CountDownLatch(1);

        Future<Integer> unresolvedFailure = new Future<>(cpu);
        unresolvedFailure.onFailure((e) -> mapTask(universalNumber, latchD).compute(42));

        Assert.assertEquals(0, universalNumber.get());

        setFailure(cpu, unresolvedFailure, new RuntimeException());

        Futures.awaitUninterruptibly(latchD);
        Assert.assertEquals(42, universalNumber.get());
    }

    @Test
    public void testCheckState()
    {
        Future<Integer> future = new Future<>(CPUs.get(0));

        try
        {
            future.checkState(Future.State.WAITING);
        }
        catch (Throwable e)
        {
            Assert.fail(e.getMessage());
        }

        try
        {
            future.checkState(Future.State.SUCCESS);
            Assert.fail();
        }
        catch (Throwable e)
        {
            Assert.assertEquals(IllegalStateException.class, e.getClass());
        }

        CountDownLatch latch = new CountDownLatch(1);

        CPUs.get(0).schedule(() -> {
            try
            {
                future.setValue(42);
            }
            finally
            {
                latch.countDown();
            }

            return null;
        });

        Futures.awaitUninterruptibly(latch);

        try
        {
            future.checkState(Future.State.SUCCESS);
        }
        catch (Throwable e)
        {
            Assert.fail(e.getMessage());
        }

        try
        {
            future.checkState(Future.State.WAITING);
            Assert.fail();
        }
        catch (Throwable e)
        {
            Assert.assertEquals(IllegalStateException.class, e.getClass());
        }

        try
        {
            future.checkState(Future.State.FAILURE);
            Assert.fail();
        }
        catch (Throwable e)
        {
            Assert.assertEquals(IllegalStateException.class, e.getClass());
        }
    }

    @Test
    public void testOnSuccessDiffCPU() throws InterruptedException {
        AtomicInteger threadId = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        Future<Integer> future = new Future<>(CPUs.get(0));
        future.onSuccess(CPUs.get(2), (v) -> {
            try
            {
                threadId.set(Integer.parseInt(Thread.currentThread().getName().split("-")[0]));
            }
            finally
            {
                latch.countDown();
            }
        });

        setValue(CPUs.get(0), future, 42);
        Futures.awaitUninterruptibly(latch);

        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(CPUs.get(2).getId(), threadId.get());
    }

    @Test
    public void testOnComplete()
    {
        CPU cpuA = CPUs.get(0);
        CPU cpuB = CPUs.get(2);

        Future<Integer> a = new Future<>(cpuA);
        Future<Integer> b = Futures.failedFuture(cpuB, new IOException());

        AtomicInteger result = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        a.onComplete(() -> {
            result.getAndAdd(21);
            latch.countDown();
        });

        // a is going to be success and b is a failure
        setValue(cpuA, a, 1);

        b.onComplete(() -> {
            result.getAndAdd(21);
            latch.countDown();
        });

        Futures.awaitUninterruptibly(latch);
        Assert.assertTrue(a.isSuccess());
        Assert.assertTrue(b.isFailure());

        Assert.assertEquals(42, result.get());
    }

    @Test
    public void testFailurePropagation()
    {
        CPU cpuA = CPUs.get(0);
        CPU cpuB = CPUs.get(2);

        RuntimeException exceptionA = new RuntimeException("a");
        RuntimeException exceptionB = new RuntimeException("b");

        CountDownLatch latchA = new CountDownLatch(2);
        CountDownLatch latchB = new CountDownLatch(3);

        // test failure propagation of already failed future
        Future<Integer> a = Futures.failedFuture(cpuA, exceptionA);
        Future<Integer> b = a.map((v) -> 0);
        Future<Integer> c = b.map(cpuB, (v) -> 1);

        b.onComplete(latchA::countDown);
        c.onComplete(latchA::countDown);

        Futures.awaitUninterruptibly(latchA);

        Assert.assertTrue(a.isFailure());
        Assert.assertTrue(b.isFailure());
        Assert.assertTrue(c.isFailure());
        Assert.assertEquals(exceptionA, a.onFailure.get());
        Assert.assertEquals(exceptionA, b.onFailure.get());
        Assert.assertEquals(exceptionA, c.onFailure.get());

        // test failure propagation of dynamically resolved future
        Future<Integer> d = new Future<>(cpuA);
        Future<Integer> e = d.map(cpuB, (v) -> 2);
        Future<Integer> f = e.map(cpuA, (v) -> 3);

        d.onComplete(latchB::countDown);
        e.onComplete(latchB::countDown);
        f.onComplete(latchB::countDown);

        setFailure(cpuA, d, exceptionB);

        Futures.awaitUninterruptibly(latchB);

        Assert.assertTrue(d.isFailure());
        Assert.assertTrue(e.isFailure());
        Assert.assertTrue(f.isFailure());
        Assert.assertEquals(exceptionB, d.onFailure.get());
        Assert.assertEquals(exceptionB, e.onFailure.get());
        Assert.assertEquals(exceptionB, f.onFailure.get());
    }

    private static <T> void setValue(CPU cpu, Future<T> future, T value)
    {
        cpu.schedule(() -> {
            future.setValue(value);
            return null;
        });
    }

    private static <T> void setFailure(CPU cpu, Future<T> future, Throwable e)
    {
        cpu.schedule(() -> {
            future.setFailure(e);
            return null;
        });
    }

    private static Task1<Integer, Void> mapTask(AtomicInteger state, CountDownLatch latch)
    {
        return (number) -> {
            try
            {
                state.addAndGet(number);
                return null;
            }
            finally
            {
                latch.countDown();
            }
        };
    }

    private static Task1<Throwable, Void> exceptionTask(AtomicReference<Throwable> state, CountDownLatch latch)
    {
        return (e) -> {
            try
            {
                state.set(e);
                return null;
            }
            finally
            {
                latch.countDown();
            }
        };
    }
}
