package io.windmill.core;

public class Status<T>
{
    public enum Flag
    {
        STOP, CONTINUE
    }

    protected final Flag flag;
    protected final T value;

    private Status(Flag flag, T value)
    {
        this.flag = flag;
        this.value = value;
    }

    public Flag getFlag()
    {
        return flag;
    }

    public T getValue()
    {
        return value;
    }

    public static <T> Status<T> of(Flag flag)
    {
        return new Status<>(flag, null);
    }

    public static <T> Status<T> of(Flag flag, T value)
    {
        return new Status<>(flag, value);
    }
}
