package io.windmill.examples.kvs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import io.windmill.core.CPU;
import io.windmill.core.CPUSet;
import io.windmill.core.Future;
import io.windmill.examples.kvs.KVStore.Get;
import io.windmill.examples.kvs.KVStore.Put;
import io.windmill.net.Channel;
import io.windmill.net.io.OutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A simple example client for {@link KVStore}
 */
public class KVClient
{
    public static void main(String[] args) throws InterruptedException, IOException
    {
        CPUSet cpus = CPUSet.builder().addSocket(0).build();
        cpus.start();

        CountDownLatch latch = new CountDownLatch(1);
        ByteBuf key = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf value = Unpooled.copyLong(System.currentTimeMillis());
        Client client = new Client(cpus.get(0), new InetSocketAddress("127.0.0.1", 31337));
        Future<ByteBuf> result = client.connect()
                .flatMap(chan -> client.put(key.duplicate(), value))
                .flatMap(prev -> client.get(key.duplicate()));

        result.onFailure(Throwable::printStackTrace);
        result.onSuccess(val -> {
            try
            {
                System.out.println("hello -> " + val.readBytes(val.readableBytes()).readLong());
            }
            finally
            {
                latch.countDown();
            }
        });

        latch.await();
        System.exit(1);
    }

    public static class Client
    {
        private final CPU cpu;
        private final InetSocketAddress host;
        private final KVStore.Get.Serializer getSerializer;
        private final KVStore.Put.Serializer putSerializer;

        private Future<Channel> connection;

        public Client(CPU cpu, InetSocketAddress host)
        {
            this.cpu = cpu;
            this.host = host;
            this.getSerializer = new KVStore.Get.Serializer();
            this.putSerializer = new KVStore.Put.Serializer();
        }

        public Future<Channel> connect()
        {
            if (connection == null)
                connection = cpu.connect(host);

            return connection;
        }

        public Future<ByteBuf> get(ByteBuf key)
        {
            Get request = new Get(key);
            ByteBuf buf = Unpooled.buffer(getSerializer.sizeOf(request) + 1);
            getSerializer.serialize(request, buf);
            return makeRequest(buf);
        }

        public Future<ByteBuf> put(ByteBuf key, ByteBuf value)
        {
            Put request = new Put(key, value);
            ByteBuf buf = Unpooled.buffer(putSerializer.sizeOf(request) + 1);
            putSerializer.serialize(request, buf);
            return makeRequest(buf);
        }

        protected Future<ByteBuf> makeRequest(ByteBuf buf)
        {
            return connection.map((channel) -> {
                OutputStream out = channel.getOutput();
                out.writeInt(buf.readableBytes()).writeAndFlush(buf);
                return channel.getInput();
            }).flatMap(input -> input.read(4).flatMap(header -> input.read(header.readInt())));
        }
    }
}
