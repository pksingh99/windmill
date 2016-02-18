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
     *
     * @param value The output value
     * @param <O> type of the output value.
     *
     * @return task producing given constant value on every call.
     */
    public static <O> Task0<O> constant(O value)
    {
        return () -> value;
    }

    /**
     * Creates a task that when computed returns the valued provided
     *
     * @param value The output value
     * @param <I> type of the argument
     * @param <O> type of the output value
     *
     * @return task producing given constant value on every call.
     */
    public static <I, O> Task1<I, O> constant1(O value)
    {
        return i -> value;
    }

    /**
     * Creates a task that when computed returns the valued provided
     *
     * @param value The output value
     * @param <I1> type of the first argument
     * @param <I2> type of the second argument
     * @param <O> type of the output value
     *
     * @return task producing given constant value on every call.
     */
    public static <I1, I2, O> Task2<I1, I2, O> constant2(O value)
    {
        return (i1, i2) -> value;
    }

    /**
     * @param <I> type of the input value
     *
     * @return input data without any transformations
     */
    public static <I> Task1<I, I> identity()
    {
        return i -> i;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     *
     * @param task The task to return back.
     * @param <O> The type of the output value.
     *
     * @return argument as cast to a task of type O, see description for details.
     */
    public static <O> Task0<O> of(Task0<O> task)
    {
        return task;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     *
     * @param task The task to return back.
     * @param <I> The type of the argument of the task
     * @param <O> The type of the output of the task.
     *
     * @return argument, see description for details.
     */
    public static <I, O> Task1<I, O> of(Task1<I, O> task)
    {
        return task;
    }

    /**
     * Takes a task and returns it. Primary reason for this is to convert lambda expressions to tasks in order
     * to access task methods.
     *
     * @param task The task to return back
     * @param <I1> type of the first argument
     * @param <I2> type of the second argument
     * @param <O> type of the output value
     *
     * @return argument, see description for details.
     */
    public static <I1, I2, O> Task2<I1, I2, O> of(Task2<I1, I2, O> task)
    {
        return task;
    }

    /**
     * Takes a curried task and returns it in normal form
     *
     * @param fn The curried task to normalize
     * @param <I1> type of the first argument
     * @param <I2> type of the second argument
     * @param <O> type of the output value
     *
     * @return normal form of the given curried task.
     */
    public static <I1, I2, O> Task2<I1, I2, O> uncurried(Task1<I1, Task1<I2, O>> fn)
    {
        return (i1, i2) -> fn.compute(i1).compute(i2);
    }
}
