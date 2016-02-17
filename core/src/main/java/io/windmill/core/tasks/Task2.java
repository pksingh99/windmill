package io.windmill.core.tasks;

@FunctionalInterface
public interface Task2<I1, I2, O>
{
    O compute(I1 i1, I2 i2);

    /**
     * Returns a task in curried form.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Currying">Currying</a>
     */
    default Task1<I1, Task1<I2, O>> curried()
    {
        return i1 -> i2 -> compute(i1, i2);
    }

    /**
     * Creates a new task that reverses the input order
     */
    default Task2<I2, I1, O> flip()
    {
        return (i2, i1) -> compute(i1, i2);
    }

    /**
     * Fixes the first argument and returns another task that represents the second argument.
     */
    default Task1<I2, O> partialApply(I1 i1)
    {
        return i2 -> compute(i1, i2);
    }
}
