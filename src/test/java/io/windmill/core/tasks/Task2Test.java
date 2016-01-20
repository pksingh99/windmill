package io.windmill.core.tasks;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task2Test
{
    private static final Logger logger = LoggerFactory.getLogger(Task2Test.class);

    @Test
    public void curried()
    {
        Task1<Integer, Integer> adder = Tasks.of(Task2Test::add).curried().compute(1);
        for (int i = 0; i < 100; i++)
            Assert.assertEquals(Integer.valueOf(i), adder.compute(i - 1));
    }

    @Test
    public void uncurried()
    {
        Task2<Integer, Integer, Integer> add = Tasks.uncurried(Tasks.of(Task2Test::add).curried());
        Assert.assertEquals(Integer.valueOf(42), add.compute(40, 2));
    }

    public static int add(int a, int b)
    {
        return a + b;
    }
}