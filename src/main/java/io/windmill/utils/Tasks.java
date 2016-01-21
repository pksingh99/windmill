package io.windmill.utils;

import io.windmill.core.tasks.Task0;
import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.Task2;

public final class Tasks
{
    private Tasks()
    {}

    /**
     * Creates a task that when computed returns the valued provided
     */
    public static <O> Task0<O> constant(O value)
    {
        return () -> value;
    }

    /**
     * Creates a task that when computed returns the valued provided
     */
    public static <I, O> Task1<I, O> constant1(O value)
    {
        return i -> value;
    }

    /**
     * Creates a task that when computed returns the valued provided
     */
    public static <I1, I2, O> Task2<I1, I2, O> constant2(O value)
    {
        return (i1, i2) -> value;
    }

    /**
     * Returns all input data without any transformations
     */
    public static <I> Task1<I, I> identity()
    {
        return i -> i;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     */
    public static <O> Task0<O> of(Task0<O> task)
    {
        return task;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     */
    public static <I, O> Task1<I, O> of(Task1<I, O> task)
    {
        return task;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     */
    public static <I1, I2, O> Task2<I1, I2, O> of(Task2<I1, I2, O> task)
    {
        return task;
    }

    /**
     * Takes a curried task and returns it in normal form
     */
    public static <I1, I2, O> Task2<I1, I2, O> uncurried(Task1<I1, Task1<I2, O>> fn)
    {
        return (i1, i2) -> fn.compute(i1).compute(i2);
    }
}
