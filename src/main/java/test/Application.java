package test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Application {

    private static final OptionParser OPTION_PARSER = new OptionParser();

    private static ArgumentAcceptingOptionSpec<Integer> portSpec = OPTION_PARSER
            .acceptsAll(Arrays.asList("p", "port"), "Bind or Connect to this port").withRequiredArg()
            .ofType(Integer.class).required();

    private static ArgumentAcceptingOptionSpec<Transport> clientTransportSpec = OPTION_PARSER
            .acceptsAll(Arrays.asList("ct", "clientTransport"), "client transport [NIO|EPOLL] (case sensitive)")
            .withRequiredArg().ofType(Transport.class).defaultsTo(Transport.NIO);

    private static ArgumentAcceptingOptionSpec<Transport> serverTransportSpec = OPTION_PARSER
            .acceptsAll(Arrays.asList("st", "serverTransport"), "server transport [NIO|EPOLL] (case sensitive)")
            .withRequiredArg().ofType(Transport.class).defaultsTo(Transport.NIO);

    public static void main(String[] args) throws InterruptedException, IOException {

        try {
            OptionSet optionSet = OPTION_PARSER.parse(args);

            if (optionSet.has("h")) {
                OPTION_PARSER.printHelpOn(System.out);
            } else {
                Application application = new Application();
                application.run(optionSet);
            }
        } catch (OptionException oe) {
            System.err.println(oe.getMessage());
            try {
                OPTION_PARSER.printHelpOn(System.out);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public void run(OptionSet optionSet) throws Exception {

        System.out.println("Test starting");

        Epoll.ensureAvailability();

        InetSocketAddress serverSocketAddress = new InetSocketAddress(optionSet.valueOf(portSpec));

        InetSocketAddress clientRemoteSocketAddress = new InetSocketAddress(optionSet.valueOf(portSpec));

        UdpEchoServer udpServer = null;
        Transport serverTransport = optionSet.valueOf(serverTransportSpec);
        switch (serverTransport) {
            case EPOLL:
                udpServer = new UdpEchoServer(serverSocketAddress, new NioEventLoopGroup(), NioDatagramChannel.class);
                break;
            case NIO:
                udpServer = new UdpEchoServer(serverSocketAddress, new EpollEventLoopGroup(),
                        EpollDatagramChannel.class);
                break;
            default:
                throw new IllegalStateException("Unrecognised server transport : " + serverTransport);
        }
        udpServer.bindAndWait();

        CountDownLatch countDownLatch = new CountDownLatch(1);

        UdpClient udpClient = null;
        Transport clientTransport = optionSet.valueOf(clientTransportSpec);
        switch (clientTransport) {
            case EPOLL:
                udpClient = new UdpClient(clientRemoteSocketAddress, countDownLatch, new EpollEventLoopGroup(),
                        EpollDatagramChannel.class);
                break;
            case NIO:
                udpClient = new UdpClient(clientRemoteSocketAddress, countDownLatch, new NioEventLoopGroup(),
                        NioDatagramChannel.class);
                break;
            default:
                throw new IllegalStateException("Unrecognised server transport : " + serverTransport);
        }
        udpClient.connect();

        byte[] msg = new byte[100];
        Arrays.fill(msg, (byte) 0x26);
        udpClient.write(msg);

        // wait for response before shutdown
        boolean responseReceived = countDownLatch.await(10, TimeUnit.SECONDS);
        if (responseReceived) {
            System.out.println("Response successfully received");
        } else {
            System.err.println("No response received");
        }

        udpClient.close();
        udpServer.close();

    }

    public enum Transport {
        NIO, EPOLL
    }

    public class UdpEchoServer {

        private final InetSocketAddress socketAddress;
        private final EventLoopGroup eventLoopGroup;
        private final Bootstrap b;
        private List<Channel> channels = new ArrayList<Channel>();

        public UdpEchoServer(InetSocketAddress socketAddress, EventLoopGroup eventLoopGroup,
                Class<? extends DatagramChannel> channelClass) {
            super();
            this.eventLoopGroup = eventLoopGroup;
            this.socketAddress = socketAddress;
            this.b = new Bootstrap();
            b.group(eventLoopGroup).channel(channelClass).handler(new ChannelInitializer<DatagramChannel>() {
                @Override
                public void initChannel(DatagramChannel ch) throws Exception {

                    ch.pipeline().addLast(new LoggingHandler("Server", LogLevel.INFO)).addLast(new EchoServerHandler());
                }
            });
        }

        public void bindAndWait() {
            channels.add(b.bind(socketAddress).syncUninterruptibly().channel());
        }

        public void close() {

            for (Channel channel : channels) {
                channel.close().awaitUninterruptibly();
            }
            eventLoopGroup.shutdownGracefully();
        }
    }

    public class EchoServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
            DatagramPacket dp = new DatagramPacket(msg.content(), msg.sender());
            msg.retain();
            ctx.writeAndFlush(dp);
        }

    }

    public class UdpClient {

        private final InetSocketAddress socketAddress;
        private final Bootstrap bootstrap;
        private final EventLoopGroup eventLoopGroup;
        private Channel channel;

        private UdpClient(InetSocketAddress socketAddress, final CountDownLatch cdl, EventLoopGroup eventLoopGroup,
                Class<? extends DatagramChannel> channelClass) {
            this.socketAddress = socketAddress;
            this.eventLoopGroup = eventLoopGroup;
            this.bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup);
            bootstrap.channel(channelClass);
            bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
                @Override
                public void initChannel(DatagramChannel ch) throws Exception {

                    ch.pipeline().addLast(new LoggingHandler("UdpClient", LogLevel.INFO))
                            .addLast(new ClientResponseHandler(cdl));
                }
            });

        }

        public void connect() throws InterruptedException {
            // Start the client.
            ChannelFuture f = bootstrap.bind(0).await();
            channel = f.channel();
        }

        public void close() throws InterruptedException {
            channel.close().awaitUninterruptibly();
            eventLoopGroup.shutdownGracefully().sync();
        }

        public void write(byte[] message) {
            DatagramPacket dp = new DatagramPacket(Unpooled.wrappedBuffer(message), socketAddress);
            channel.writeAndFlush(dp).awaitUninterruptibly();
        }

        public class ClientResponseHandler extends SimpleChannelInboundHandler<DatagramPacket> {

            private final CountDownLatch cdl;

            public ClientResponseHandler(CountDownLatch cdl) {
                super();
                this.cdl = cdl;
            }

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                cdl.countDown();
            }

        }
    }

}
