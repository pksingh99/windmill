package io.windmill.core.tasks;

@FunctionalInterface
public interface VoidTask<I>
{
    void compute(I input);
}
