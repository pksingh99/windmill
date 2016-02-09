package io.windmill.examples.kvs;

import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.windmill.core.CPU;
import io.windmill.core.CPUSet;
import io.windmill.core.Future;
import io.windmill.disk.File;
import io.windmill.disk.FileContext;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * An example implementation of a persistent key-value store.
 *
 * Each cpu is assigned a bucket of keys. For each bucket a commit log
 * is maintained, as well as an in-memory copy of the current value of
 * each key (persistence is demonstration-only and commit logs are thrown away
 * on restart). Access to the store is available via a simple network protocol.
 * See {@link KVClient} for an example client implementation.
 *
 * One CPU is also responsible for listening on, and reading from/writing
 * to the store's network socket. Requests originate (bytes read/parsed) and
 * terminate (bytes written) on this CPU but they are processed on the CPU
 * which the bucket the key belongs to is assigned (using cpu.schedule or
 * the file operations provided by windmill).
 */
public class KVStore
{
    public static void main(String[] args) throws InterruptedException
    {
        CPUSet cpus = CPUSet.builder().addSocket(0, 1, 2, 3).build();

        cpus.start();

        AppConfig config = new AppConfig(cpus);

        cpus.get(0).listen(new InetSocketAddress("127.0.0.1", 31337), (channel) -> {
            InputStream  in  = channel.getInput();
            OutputStream out = channel.getOutput();

            System.out.println("connected => " + channel);
            channel.loop((cpu, prev) -> in.read(4)
                                          .flatMap((header) -> in.read(header.readInt()))
                                          .flatMap(config::route)
                                          .map(cpu, (response) -> out.writeInt(response.readableBytes())
                                                                     .writeBytes(response)
                                                                     .flush()));
        }, Throwable::printStackTrace);

        Thread.currentThread().join();
        System.exit(1);
    }

    private static class AppConfig
    {
        private final List<Bucket> buckets = new ArrayList<>();

        public AppConfig(CPUSet cpus)
        {
            cpus.forEach((cpu) -> {
                try
                {
                    java.io.File tmp = java.io.File.createTempFile(String.format("cpu-%d-", cpu.getId()), ".log");
                    tmp.deleteOnExit();

                    System.out.printf("Allocated CL at %s for CPU %d%n", tmp.getAbsolutePath(), cpu.getId());
                    buckets.add(new Bucket(tmp.getAbsolutePath(), cpu));
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            });
        }

        public Future<ByteBuf> route(ByteBuf request)
        {
            Request req = getSerializer(request.getByte(0)).deserialize(request);

            Bucket bucket = bucketFor(req.getKey());

            Future<ByteBuf> response = new Future<>(bucket.cpu);
            Future<ByteBuf> resultFuture = req.execute(bucket);

            resultFuture.onSuccess(response::setValue);
            resultFuture.onFailure((e) -> {
                e.printStackTrace();
                response.setValue(Unpooled.EMPTY_BUFFER);
            });

            return response;
        }

        public RequestSerializer<?> getSerializer(byte type)
        {
            return Request.Type.valueOf(type).serializer;
        }

        public Bucket bucketFor(ByteBuf key)
        {
            return buckets.get(Math.abs(key.hashCode()) % buckets.size());
        }
    }

    private static class Bucket
    {
        private final CPU cpu;
        private final Map<ByteBuf, ByteBuf> store;
        private final Future<File> commitLog;

        private Future<FileContext> currentCLContext;

        public Bucket(String commitLogPath, CPU cpu)
        {
            this.cpu = cpu;
            this.store = new HashMap<>();
            this.commitLog = cpu.open(commitLogPath, "rw");
            this.currentCLContext = commitLog.flatMap((file) -> file.seek(0));
            this.cpu.repeat((CPU bucketCPU, Future prev) -> prev != null && prev.isFailure()
                                        ? prev // returns a failure which stops the loop
                                        : cpu.sleep(1, TimeUnit.SECONDS, () -> commitLog.map(File::sync)));
        }

        public Future<ByteBuf> put(Put put)
        {
            // all file operations are performed serially by the cpu this
            // bucket is running on so re-assignment here is safe
            currentCLContext = put.checkpoint(currentCLContext);
            return currentCLContext.map((context) -> {
                ByteBuf prev = store.put(put.key, put.value);
                return prev == null ? Unpooled.EMPTY_BUFFER : prev;
            });
        }

        public Future<ByteBuf> get(Get get)
        {
            return cpu.schedule(() -> {
                ByteBuf value = store.get(get.key);
                return value == null ? Unpooled.EMPTY_BUFFER : value;
            });
        }
    }

    public static abstract class Request
    {
        public enum Type
        {
            PUT((byte) 1, new Put.Serializer()), GET((byte) 2, new Get.Serializer());

            public final byte marker;
            public final RequestSerializer serializer;

            Type(byte marker, RequestSerializer serializer)
            {
                this.marker = marker;
                this.serializer = serializer;
            }

            public static Type valueOf(byte type)
            {
                switch (type)
                {
                    case 1:
                        return PUT;

                    case 2:
                        return GET;

                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }

        protected final Type type;
        protected final ByteBuf key;

        public Request(Type type, ByteBuf key)
        {
            this.type = type;
            this.key = key;
        }

        public ByteBuf getKey()
        {
            return key;
        }

        public abstract Future<ByteBuf> execute(Bucket bucket);
    }

    public static class Put extends Request
    {
        private final ByteBuf value;

        public Put(ByteBuf key, ByteBuf value)
        {
            super(Type.PUT, key);
            this.value = value;
        }

        @Override
        public Future<ByteBuf> execute(Bucket bucket)
        {
            return bucket.put(this);
        }

        public Future<FileContext> checkpoint(Future<FileContext> contextFuture)
        {
            ByteBuf out = Unpooled.buffer();
            type.serializer.serialize(this, out);
            return contextFuture.flatMap((context) -> context.write(out));
        }

        public static class Serializer implements RequestSerializer<Put>
        {
            @Override
            public void serialize(Put put, ByteBuf out)
            {
                out.writeByte(put.type.marker);

                out.writeInt(put.key.readableBytes());
                out.writeBytes(put.key.duplicate());

                out.writeInt(put.value.readableBytes());
                out.writeBytes(put.value.duplicate());
            }

            @Override
            public Put deserialize(ByteBuf in)
            {
                if (in.readByte() != Type.PUT.marker)
                    throw new IllegalStateException();

                ByteBuf key   = in.readBytes(in.readInt());
                ByteBuf value = in.readBytes(in.readInt());

                return new Put(key, value);
            }

            public int sizeOf(Put put)
            {
                return 1 + 4 + put.key.readableBytes() + 4 + put.value.readableBytes();
            }
        }
    }

    public static class Get extends Request
    {
        public Get(ByteBuf key)
        {
            super(Type.GET, key);
        }

        @Override
        public Future<ByteBuf> execute(Bucket bucket)
        {
            return bucket.get(this);
        }

        public static class Serializer implements RequestSerializer<Get>
        {
            @Override
            public void serialize(Get get, ByteBuf out)
            {
                out.writeByte(Type.GET.marker);

                out.writeInt(get.key.readableBytes());
                out.writeBytes(get.key);
            }

            @Override
            public Get deserialize(ByteBuf in)
            {
                if (in.readByte() != Type.GET.marker)
                    throw new IllegalStateException();

                return new Get(in.readBytes(in.readInt()));
            }

            @Override
            public int sizeOf(Get get)
            {
                return 1 + 4 + get.key.readableBytes();
            }
        }
    }

    public interface RequestSerializer<T extends Request>
    {
        void serialize(T request, ByteBuf out);
        T deserialize(ByteBuf in);
        int sizeOf(T request);
    }
}
