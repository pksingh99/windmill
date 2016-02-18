package io.windmill.core.tasks;

@FunctionalInterface
public interface Task1<I, O>
{
    O compute(I input);

    /**
     * Takes the result of calling {@link #compute(Object)} on the current task then applies the result as input to
     * the provided task.
     *
     * @param fn The task to transform upon completion
     * @param <C> The type of the new output
     *
     * @return The transformation performed on original task.
     */
    default <C> Task1<I, C> andThen(Task1<O, C> fn)
    {
        return a -> fn.compute(compute(a));
    }

    /**
     * Takes the result of calling {@link #compute(Object)} on the input function then applies the result as input
     * to this task.
     *
     * @param fn The composition function
     * @param <C> The type of the new output
     *
     * @return The result of composition of current and given functions
     */
    default <C> Task1<C, O> compose(Task1<C, I> fn)
    {
        return a -> compute(fn.compute(a));
    }

    /**
     * Fixes the first argument and returns another task that when computed is the same as calling compute with
     * the input
     *
     * @param i The input of the new task
     *
     * @return New task where input argument have been fixed to given value.
     */
    default Task0<O> apply(I i)
    {
        return () -> compute(i);
    }
}

