# Windmill

[![Build Status](https://travis-ci.org/xedin/windmill.svg?branch=master)](https://travis-ci.org/xedin/windmill)

Windmill is a Java 8 library for building high-performance
applications using the "Thread-per-Core" or "Shared-Nothing"
architecture. It is hosted on
[Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.xedin%7Cwindmill-core%7C0.1%7Cjar):

```
<dependency>
  <groupId>com.github.xedin</groupId>
  <artifactId>windmill-core</artifactId>
  <version>0.1</version>
</dependency>
```

## Background

Recent trends in commodity hardware (increased core count over
increased core speed, NUMA, storage on the PCI bus, etc) have required
a change in architecture for applications wishing to achieve the best
performance on the hardware on which it is being run. While
asynchronous programming has become popular in recent years, alone it
is insufficient. Single-threaded event-loop models pose a challenge
for developers and operators trying to use every core on a machine,
often requiring multiple processes to be run, limiting the ability to
communicate sans IPC and increasing operations/management
overhead. Meanwhile, multi-threaded event loops, or similar
architectures like common SEDA implementations in Java, that leave
scheduling to the operating system, and potentially use many many
threads, don't take advantage of NUMA or leave it to the operating
system. They may also pay a high context switching cost. Windmill
provides the advantages of both approaches while trying to minimize
some of the downsides (e.g. by providing a familiar programming model
for an unfamiliar architecture).

## Concepts, Usage, and Examples

A simple, networked key-value store and client example can be found
[here](https://github.com/xedin/windmill/tree/master/src/main/java/io/windmill/examples/kvs). The
following describes the components of windmill and how to use them.

### CPU and CPUSets

The `CPU` is the primary execution abstraction in windmill. Each
instance maintains its own run queue, network, and IO
services. Multiple `CPU`s can be running, executing work, at any given
time, and, if the underlying operating system supports it, each `CPU`
has its affinity set to a physical core on the machine. Work is
scheduled on a specific `CPU` and work performed on one `CPU` can
explicitly schedule more work on other `CPU`s. Hyper-Threading is
explicitly disabled, when affinities are set, preventing jitter and
unnecessary resource contention.

Every windmill application contains one or more, but typically only
one,
[`CPUSet`](https://github.com/xedin/windmill/blob/master/src/main/java/io/windmill/core/CPUSet.java). `CPUSet`s
provide an interface to manage and run multiple `CPU`s.

### Futures and Promises

Windmill applications are programmed using a familiar and convenient
asynchronous programming model: `Future`s and `Promise`s. Like
`java.lang.concurrent.Future`, a windmill `Future` represents a
computation that may be completed or that will complete at a later
time.  The interface allows applications to schedule future work based
on the result of the previous asynchronous task. The `Promise`,
typically not used by the windmill application programmer, is the
implementation that performs the work, making the result available to
the `Future`.

Nearly every operation provided by windmill returns a `Future<T>`,
where `T` represents the result type. This includes network, disk, and
ad-hoc work scheduled on a `CPU`.

Where windmill's `Future`s are different from existing JVM `Future`
implementations is in an extra, optional, argument to functions that
allow scheduling of future work based on the `Future`s result, like
`map` or `flatMap`. The `remoteCPU` argument, allows the work to be
scheduled on a different `CPU` than where the `Future` originally
executed, allowing for granular control over where pieces of the
application execute.

### Scheduling and Performing Work

The `CPU.schedule()` function allows for performing arbitrary work,
expressed as a Java 8 lambda:

```java
CPU cpu = ...
cpu.schedule(() -> System.out.println("hello world!"));
```

The schedule function returns a `Future<T>`, where `T` is the return
type of the lambda expression (in this case `Void`), which can be used
to schedule more work as a result of the completion of the first
task. The example above could also be written in this more verbose form:

```java
cpu.schedule(() -> System.out.print("hello ")).onComplete(() -> System.out.println("world!"));
```

### Network

To provide support for networked applications, each windmill `CPU`
maintains its own set of network sockets, which can be opened using
functions defined on `CPU` instances. Asynchronous operations are
implemented using java's `Selector` and `SocketChannel`
interfaces. Once opened, applications can perform socket operations on
`io.windmill.net.Channel`.

To listen for new TCP connections on a given host and port use
`CPU.listen()`, which takes an `InetSocketAddress` and two lambdas --
one to handle newly accepted connections and the other when there is an
exception. The following example prints a message for each new
connection or the stacktrace of the failure if there is one.

```java
CPU cpu = ...
cpu.listen(new InetSocketAddress("127.0.0.1", 8080),  (channel) -> {
  System.out.println("connected => " + channel);
}, Throwable::printStackTrace);
```

To connect to a socket use `CPU.connect`, which returns a
`Future<Channel>`, that can be used like any other windmill `Future`.

Once a reference to a `Channel` has been obtained, reading and writing
can be performed via the `InputStream` and `OutputStream` available
via `Channel.getOutput` and `Channel.getInput`, respectively. The
`Input/OutputStream` API supports both fixed size reads and writes,
and consuming from the socket as a stream of bytes as they
arrive. The `OutputStream` also supports explicit flushing, enabling
more control over when bytes are written to the network. An example of
their use can be found
[here](https://github.com/xedin/windmill/tree/master/src/main/java/io/windmill/examples/kvs).

The following, simple example illustrates reading a given number of
integers from the stream, returning their sum. The first integer (four
bytes) represents the length of the message, which once read, provides
the number of bytes to read from the stream (passed to
`input::read`). The bytes read are then interpreted as integers and
summed before being written back to the `OutputStream`.


```java
InputStream input = c.getInput();
OutputStream output = c.getOutput();

c.loop((cpu) ->
  input.readInt()
       .flatMap(input::read)
       .map((msg) -> {
         int sum = 0;
         while (msg.readableBytes() > 0)
           sum += msg.readInt();
         return output.writeInt(sum).flush();
}));
```

### Timers

Windmill also supports scheduling delayed work via the `CPU.sleep` function.
A delayed Hello World example could be written as:

```java
CPU cpu = ...
cpu.sleep(1, TimeUnit.SECONDS, () -> System.out.println("hello, world!"));
```

Or, using the `Future` returned by `sleep`:

```java
Future<String> f = cpus.get(0).sleep(1, TimeUnit.SECONDS, () -> "hello ");
f.onSuccess((s) -> System.out.println(s + "world!"));
```

### Disk

For each `CPU`, windmill maintains its own pool of I/O threads for
performing disk operations. A page cache, similar to the page cache in
the Linux kernel, is also maintained per `CPU`, which avoids copies, enables more
efficient data retrieval, coalesces reads & writes, and reduces overhead on
explicit flushes.  If the `CPU`s affinity is set, the affinity for
these threads is set to the same physical core. By isolating Disk I/O
to a core, the machine can better use its local CPU caches. The `CPU`
provides two ways to perform work on these threads: the direct
approach, `CPU.scheduleIO`, or indirectly via operations on file's
opened by `CPU.open`.

`CPU.scheduleIO` is similar to `CPU.schedule` but the work is
performed on one of the disk I/O threads instead of being executed as
part of the `CPU`'s main run-loop. The `Future<File>` returned by
`open` and the `FileContext` provided by `File.seek` provide an
interface to perform operations asynchronously on the opened file. All
of these operations are performed on the disk I/O threads. The following
example repeatedly appends to disk, flushing once per second.

```java
java.io.File tmp = java.io.File.createTempFile("hello", "world");
tmp.deleteOnExit();
Future<File> fileFuture = cpus.get(0).open(tmp, "rw");
cpus.get(0).repeat((CPU cpu, FileContext prev) -> {
  Future<FileContext> context = prev == null
                              ? fileFuture.flatMap((f) -> f.seek(0))
                              : Futures.constantFuture(cpu, prev);

  return context.flatMap((c) -> c.write("hello world\n".getBytes()))
                .map((c) -> Status.of(Status.Flag.CONTINUE, c));
});

cpus.get(0).repeat(cpu -> cpu.sleep(1, TimeUnit.SECONDS, () -> fileFuture.map(File::sync))
           .map(n -> Status.of(Status.Flag.CONTINUE)));
```

In this example, `CPU.open` is used to asynchronously open the file,
and `seek` is used to obtain the first reference to the
`FileContext`. `FileContext` provides the current position in the file
and the ability to write at that position without knowing it. The
`FileContext` from `write` is propagated to the next iteration by
passing it to `Status.of`. The second call to `repeat` shows how it
can be used for work that only produces side-effects as well.

## LICENSE

Windmill is BSD Licensed. See the `LICENSE` file for more details.

## Contributors

See the `CONTRIBUTORS` file.
