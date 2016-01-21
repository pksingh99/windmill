package io.windmill.core.tasks;

import io.windmill.utils.Tasks;

import org.junit.Assert;
import org.junit.Test;

public class Task1Test
{
    @Test
    public void andThen()
    {
        Task1<String, Integer> stringLeadingZeros = Tasks.of(Task1Test::size).andThen(Integer::numberOfLeadingZeros);
        Assert.assertEquals(Integer.valueOf(Integer.numberOfLeadingZeros(size("Hello World"))), stringLeadingZeros.compute("Hello World!"));
    }

    @Test
    public void compose()
    {
        Task1<Integer, Integer> task = Tasks.of(Task1Test::size).compose(Integer::toBinaryString);
        Assert.assertEquals(Integer.valueOf(size(Integer.toBinaryString(42))), task.compute(42));
    }

    public static int size(String value)
    {
        return value.length();
    }

}