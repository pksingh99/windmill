package com.apple.akane.core.tasks;

@FunctionalInterface
public interface Task1<I, O>
{
    O compute(I input);
}

