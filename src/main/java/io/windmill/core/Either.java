package io.windmill.core;

import com.google.common.base.Objects;
import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.VoidTask;

/**
 * Represents that a value can be in one of two states, but not both at the same time.
 *
 * This class leans towards the left, meaning that all combinator assume left is present, else they are seen as failed.
 * If this assumption is not true, then a {@link #swap()} ()} must be called to swap.
 */
public abstract class Either<L, R>
{
    // hide constructor so only left and right can impl
    private Either() {}

    public abstract boolean isLeft();

    public abstract boolean isRight();

    public abstract <A> Either<A, R> map(Task1<L, A> task);

    public abstract <A> Either<A, R> flatMap(Task1<L, Either<A, R>> task);

    public abstract void foreach(VoidTask<L> task);

    public abstract Either<R, L> swap();

    public Left<L, R> toLeft()
    {
        return (Left<L, R>) this;
    }

    public Right<L, R> toRight()
    {
        return (Right<L, R>) this;
    }

    public static <L, R> Either<L, R> left(L value)
    {
        return new Left<>(value);
    }

    public static <L, R> Either<L, R> right(R value)
    {
        return new Right<>(value);
    }

    public static final class Left<L, R> extends Either<L, R>
    {
        private final L value;

        public Left(L value)
        {
            this.value = value;
        }

        public L getValue()
        {
            return value;
        }

        @Override
        public boolean isLeft()
        {
            return true;
        }

        @Override
        public boolean isRight()
        {
            return false;
        }

        @Override
        public <A> Either<A, R> map(Task1<L, A> task)
        {
            return left(task.compute(value));
        }

        @Override
        public <A> Either<A, R> flatMap(Task1<L, Either<A, R>> task)
        {
            return task.compute(value);
        }

        @Override
        public void foreach(VoidTask<L> task)
        {
            task.compute(value);
        }

        @Override
        public Either<R, L> swap()
        {
            return right(value);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Left(");
            sb.append(value).append(")");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Left<?, ?> left = (Left<?, ?>) o;
            return Objects.equal(value, left.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(value);
        }
    }

    public static final class Right<L, R> extends Either<L, R>
    {
        private final R value;

        public Right(R value)
        {
            this.value = value;
        }

        public R getValue()
        {
            return value;
        }

        @Override
        public boolean isLeft()
        {
            return false;
        }

        @Override
        public boolean isRight()
        {
            return true;
        }

        @Override
        public <A> Either<A, R> map(Task1<L, A> task)
        {
            return (Either<A, R>) this;
        }

        @Override
        public <A> Either<A, R> flatMap(Task1<L, Either<A, R>> task)
        {
            return (Either<A, R>) this;
        }

        @Override
        public void foreach(VoidTask<L> task)
        {
            // no-op
        }

        @Override
        public Either<R, L> swap()
        {
            return left(value);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Right(");
            sb.append(value).append(")");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Right<?, ?> right = (Right<?, ?>) o;
            return Objects.equal(value, right.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(value);
        }
    }
}
