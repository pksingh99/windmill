package io.windmill.core.tasks;

@FunctionalInterface
public interface Task0<O>
{
    O compute();
}

