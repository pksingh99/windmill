package io.windmill.core;

import io.windmill.core.tasks.Task0;

public class Promise<O>
{
    protected final Future<O> future;
    protected final Task0<O> task;

    public Promise(CPU cpu, Task0<O> task)
    {
        this(new Future<>(cpu), task);
    }

    public Promise(Future<O> future, Task0<O> task)
    {
        this.future = future;
        this.task = task;
    }

    public Future<O> getFuture()
    {
        return future;
    }

    protected void fulfil()
    {
        future.checkState(Future.State.WAITING);

        try
        {
            future.setValue(task.compute());
        }
        catch (Throwable o)
        {
            future.setFailure(o);
        }
    }

    protected void scheduleFailure(Throwable e)
    {
        future.cpu.schedule(() -> {
            future.checkState(Future.State.WAITING);
            future.setFailure(e);
            return null;
        });
    }

    public void schedule()
    {
        future.cpu.schedule(this);
    }
}