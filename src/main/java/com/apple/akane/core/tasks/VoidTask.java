package com.apple.akane.core.tasks;

@FunctionalInterface
public interface VoidTask<I>
{
    void compute(I input);
}
