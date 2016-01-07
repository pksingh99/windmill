package io.windmill.core.tasks;

@FunctionalInterface
public interface Task1<I, O>
{
    O compute(I input);
}

