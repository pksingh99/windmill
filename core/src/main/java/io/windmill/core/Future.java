package io.windmill.core;

import java.util.ArrayDeque;
import java.util.Queue;

import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.VoidTask0;
import io.windmill.core.tasks.VoidTask1;

/**
 * A container for asynchronous work that may have completed or will
 * complete at a later time either by producing a value or by ending in an
 * exception, similar to {@link java.util.concurrent.Future} and
 * implementations in other languages or libraries. Windmill {@link Future}s
 * run on a specific {@link CPU} and operations to schedule work based on the
 * result of one {@link Future} can be scheduled on the same or a different
 * {@link CPU}.
 *
 * @param <O> the type of value the work produces
 */
public class Future<O>
{
    public enum State
    {
        WAITING, SUCCESS, FAILURE
    }

    protected final CPU cpu;
    protected final Future<Throwable> onFailure;
    protected final Queue<Promise<?>> continuations = new ArrayDeque<>();

    private State state = State.WAITING;
    private O value;

    public Future(CPU cpu)
    {
        this(cpu, new Future<>(cpu, null));
    }

    protected Future(CPU cpu, Future<Throwable> onFailure)
    {
        this.cpu = cpu;
        this.onFailure = onFailure;
    }

    /**
     * Indicates if the work has completed, either by producing a result or ending in exception
     * @return true if either {@link #isSuccess()} or {@link #isFailure()} is true
     */
    public boolean isAvailable()
    {
        return isSuccess() || isFailure();
    }

    /**
     * @return true when the work completed successfully, producing a value
     */
    public boolean isSuccess()
    {
        return state == State.SUCCESS;
    }

    /**
     * @return true when the work ended in exception
     */
    public boolean isFailure()
    {
        return state == State.FAILURE;
    }

    protected O get()
    {
        if (!isAvailable())
            throw new IllegalStateException("future is not yet available.");

        return value;
    }

    /**
     * Sets the result value of this {@link Future} and sets the state to {@code SUCCESS}
     *
     * @param newValue the result of the work performed
     * @throws IllegalStateException if either {@link #setValue(Object)} or {@link #setFailure(Throwable)} has been called
     */
    public void setValue(O newValue)
    {
        setValue(State.SUCCESS, newValue);
    }

    protected void setValue(State newState, O newValue)
    {
        checkState(State.WAITING);

        state = newState;
        value = newValue;

        while (!continuations.isEmpty())
            continuations.poll().schedule();
    }

    /**
     * Sets the result of this {@link Future} to the given {@link Throwable} and
     * sets the state to {@code FAILURE}.
     *
     * @throws IllegalStateException if either {@link #setValue(Object)} or {@link #setFailure(Throwable)} has been called
     * @param e the exception that occurred while performing the work
     */
    public void setFailure(Throwable e)
    {
        checkState(State.WAITING);

        state = State.FAILURE;
        onFailure.setValue(e);

        while (!continuations.isEmpty())
            continuations.poll().scheduleFailure(e);
    }

    protected void checkState(State requiredState)
    {
        if (state != requiredState)
            throw new IllegalStateException("required " + requiredState + " but was " + state);
    }

    /**
     * Transform the value produced by this {@link Future}. The transformation is applied
     * on the CPU that the work this {@link Future} represents is running on.
     *
     * @param continuation the function to apply to the result of this {@link Future}
     * @param <T> the type of value returned by the function
     * @return a {@link Future} whose result, when available, will reflect the applied transformation
     */
    public <T> Future<T> map(Task1<O, T> continuation)
    {
        return map(cpu, continuation);
    }

    /**
     * Transform the value produced by this {@link Future}, performing the work
     * on the given {@link CPU}, instead of the {@link CPU} this {@link Future}'s work
     * is executing on
     *
     * @param remoteCPU the {@link CPU} to apply the transformation on
     * @param continuation the transformation to apply
     * @param <T> the type of value produced by the transformation
     * @return a {@link Future} whose result, when available, will reflect the applied transformation
     */
    public <T> Future<T> map(CPU remoteCPU, Task1<O, T> continuation)
    {
        Promise<T> promise = new Promise<>(remoteCPU, () -> continuation.compute(get()));

        cpu.schedule(() -> {
            attach(promise);
            return null;
        });

        return promise.getFuture();
    }

    /**
     * Perform another asynchronous body of work based on the result of this
     * {@link Future}'s work. The work will be performed on the same {@link CPU}
     * as this {@link Future}'s.
     *
     * @param continuation the work to perform
     * @param <T> the type of value returned by the work
     * @return a {@link Future} that can be used to operate on the final result
     */
    public <T> Future<T> flatMap(Task1<O, Future<T>> continuation)
    {
        return flatMap(cpu, continuation);
    }

    /**
     * Perform another asynchronous body of work based on the result of this
     * {@link Future}'s work. The work will be performed on the given {@link CPU}
     * instead of on the same {@link CPU} that is running the work represented by this
     * {@link Future}.
     *
     * @param remoteCPU the {@link CPU} to perform the work on
     * @param continuation the work to perform
     * @param <T> the type of value produced by the work
     * @return a {@link Future} that can be used to operate on the final result
     */
    public <T> Future<T> flatMap(CPU remoteCPU, Task1<O, Future<T>> continuation)
    {
        Future<T> sink = new Future<>(cpu);
        map(remoteCPU, (o) -> {
            Future<T> future = continuation.compute(o);
            future.onSuccess(sink::setValue);
            future.onFailure(sink::setFailure);
            return null;
        });
        return sink;
    }

    /**
     * Execute work when this {@link Future}'s work completes successfully. The work
     * produces no value and is executed on the same {@link CPU} as this {@link Future}'s
     * work.
     * @param then the work to execute
     */
    public void onSuccess(VoidTask1<O> then)
    {
        map((o) -> { then.compute(o); return null; });
    }

    /**
     * Execute work when this {@link Future}'s work completes successfully. The work
     * produces no value and is executed on the given {@link CPU} instead of on the {@link CPU}
     * this {@link Future}'s work is being executed on.
     * @param remoteCPU the {@link CPU} to perform the work on
     * @param then the work to perform
     */
    public void onSuccess(CPU remoteCPU, VoidTask1<O> then)
    {
        map(remoteCPU, (o) -> { then.compute(o); return null; });
    }

    /**
     * Execute work when this {@link Future}'s work ends in an exception. The work
     * produces no value and is executed on the same {@link CPU} as this {@link Future}'s
     * work.
     * @param continuation the work to execute
     */
    public void onFailure(VoidTask1<Throwable> continuation)
    {
        onFailure.onSuccess(continuation);
    }

    /**
     * Execute work regardless of whether or not the work succeeds or fails
     * @param continuation the work to execute
     */
    public void onComplete(VoidTask0 continuation)
    {
        onSuccess((o) -> continuation.compute());
        onFailure((e) -> continuation.compute());
    }

    private void attach(Promise<?> continuation)
    {
        if (isSuccess())
            continuation.schedule();
        else if (isFailure())
            continuation.scheduleFailure(onFailure.get());
        else
            continuations.add(continuation);
    }
}