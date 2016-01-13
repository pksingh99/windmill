package io.windmill.core.tasks;

@FunctionalInterface
public interface VoidTask1<I>
{
    void compute(I input);
}
