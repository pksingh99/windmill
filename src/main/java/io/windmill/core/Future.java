package io.windmill.core;

import java.util.ArrayDeque;
import java.util.Queue;

import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.VoidTask0;
import io.windmill.core.tasks.VoidTask1;

public class Future<O>
{
    public enum State
    {
        WAITING, SUCCESS, FAILURE
    }

    protected final CPU cpu;
    protected final Future<Throwable> onFailure;
    protected final Queue<Promise<?>> continuations = new ArrayDeque<>();

    private State state = State.WAITING;
    private O value;

    public Future(CPU cpu)
    {
        this(cpu, new Future<>(cpu, null));
    }

    protected Future(CPU cpu, Future<Throwable> onFailure)
    {
        this.cpu = cpu;
        this.onFailure = onFailure;
    }

    public boolean isAvailable()
    {
        return isSuccess() || isFailure();
    }

    public boolean isSuccess()
    {
        return state == State.SUCCESS;
    }

    public boolean isFailure()
    {
        return state == State.FAILURE;
    }

    protected O get()
    {
        if (!isAvailable())
            throw new IllegalStateException("future is not yet available.");

        return value;
    }

    public void setValue(O newValue)
    {
        setValue(State.SUCCESS, newValue);
    }

    protected void setValue(State newState, O newValue)
    {
        checkState(State.WAITING);

        state = newState;
        value = newValue;

        // don't run success/map continuations on failure
        if (state == State.FAILURE)
        {
            continuations.clear();
            return;
        }

        while (!continuations.isEmpty())
            continuations.poll().schedule();
    }

    public void setFailure(Throwable e)
    {
        setValue(State.FAILURE, null);
        onFailure.setValue(e);
    }

    protected void checkState(State requiredState)
    {
        if (state != requiredState)
            throw new IllegalStateException("required " + requiredState + " but was " + state);
    }

    public <T> Future<T> map(Task1<O, T> continuation)
    {
        return map(cpu, continuation);
    }

    public <T> Future<T> map(CPU remoteCPU, Task1<O, T> continuation)
    {
        Promise<T> promise = new Promise<>(remoteCPU, () -> continuation.compute(get()));

        cpu.schedule(() -> {
            attach(promise);
            return null;
        });

        return promise.getFuture();
    }

    public <T> Future<T> flatMap(Task1<O, Future<T>> continuation)
    {
        Future<T> sink = new Future<>(cpu);
        map(cpu, (o) -> {
            Future<T> future = continuation.compute(o);
            future.onSuccess(sink::setValue);
            future.onFailure(sink::setFailure);
            return null;
        });
        return sink;
    }

    public void onSuccess(VoidTask1<O> then)
    {
        map((o) -> { then.compute(o); return null; });
    }

    public void onSuccess(CPU remoteCPU, VoidTask1<O> then)
    {
        map(remoteCPU, (o) -> { then.compute(o); return null; });
    }

    public void onFailure(VoidTask1<Throwable> continuation)
    {
        onFailure.onSuccess(continuation);
    }

    public void onComplete(VoidTask0 continuation)
    {
        onSuccess((o) -> continuation.compute());
        onFailure((e) -> continuation.compute());
    }

    private void attach(Promise<?> continuation)
    {
        if (isSuccess())
            continuation.schedule();

        continuations.add(continuation);
    }
}