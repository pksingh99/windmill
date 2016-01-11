package io.windmill.core;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EitherTest {
    private static final Logger logger = LoggerFactory.getLogger(EitherTest.class);

    @Test
    public void leftish()
    {
        String expected = "Hello World!";
        Either<String, Integer> e = Either.<String, Integer>left(expected);
        assertLeft(e, expected);
    }

    @Test
    public void rightish()
    {
        Integer expected = 42;
        Either<String, Integer> e = Either.<String, Integer>right(expected);
        assertRight(e, expected);
    }

    @Test
    public void mapLeft()
    {
        String value = "Hello World!";
        Integer expected = value.length();
        Either<Integer, String> e = Either.<String, String>left(value).map(x -> x.length());
        assertLeft(e, expected);
    }

    @Test
    public void mapRight()
    {
        Integer value = 42;
        Either<String, Integer> e = Either.right(value);
        // verify first map does nothing
        e.map(x -> x).foreach(Assert::fail);
        // verify swap then map works
        assertLeft(e.swap().map(x -> x / 2), value / 2);
    }

    @Test
    public void flatmapLeft()
    {
        String value = "Hello World!";
        Either<Integer, Throwable> e = Either.<String, Throwable>left(value).flatMap(x -> Either.<Integer, Throwable>left(x.length()));
        assertLeft(e, value.length());
    }

    @Test
    public void flatmapRight()
    {
        String value = "Hello World!";
        Either<Throwable, String> e = Either.right(value);
        // verify first map does nothing
        e.flatMap(t -> Either.left(t.getMessage())).foreach(Assert::fail);
        // verify swap then map works
        assertLeft(e.swap().flatMap(x -> Either.left(x.length())), value.length());
    }

    public static <L, R> void assertLeft(Either<L, R> e, L expected)
    {
        logger.info("Either {}", e);
        Assert.assertTrue(e.isLeft());
        Assert.assertFalse(e.isRight());
        Assert.assertEquals(expected, e.toLeft().getValue());
    }

    public static <L, R> void assertRight(Either<L, R> e, R expected)
    {
        logger.info("Either {}", e);
        Assert.assertFalse(e.isLeft());
        Assert.assertTrue(e.isRight());
        Assert.assertEquals(expected, e.toRight().getValue());
    }

}