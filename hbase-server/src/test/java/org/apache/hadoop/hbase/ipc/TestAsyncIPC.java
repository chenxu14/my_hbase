/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;

@RunWith(Parameterized.class)
@Category({ SmallTests.class })
public class TestAsyncIPC extends AbstractTestIPC {

  private static final Log LOG = LogFactory.getLog(TestAsyncIPC.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    paramList.add(new Object[] { false, false });
    paramList.add(new Object[] { false, true });
    paramList.add(new Object[] { true, false });
    paramList.add(new Object[] { true, true });
    return paramList;
  }

  private final boolean useNativeTransport;

  private final boolean useGlobalEventLoopGroup;

  public TestAsyncIPC(boolean useNativeTransport, boolean useGlobalEventLoopGroup) {
    this.useNativeTransport = useNativeTransport;
    this.useGlobalEventLoopGroup = useGlobalEventLoopGroup;
  }

  private void setConf(Configuration conf) {
    conf.setBoolean(AsyncRpcClient.USE_NATIVE_TRANSPORT, useNativeTransport);
    conf.setBoolean(AsyncRpcClient.USE_GLOBAL_EVENT_LOOP_GROUP, useGlobalEventLoopGroup);
    if (useGlobalEventLoopGroup && AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP != null) {
      if (useNativeTransport
          && !(AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP.getFirst() instanceof EpollEventLoopGroup)
          || (!useNativeTransport
          && !(AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP.getFirst() instanceof NioEventLoopGroup))) {
        AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP.getFirst().shutdownGracefully();
        AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP = null;
      }
    }
  }

  @Override
  protected AsyncRpcClient createRpcClientNoCodec(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf) {

      @Override
      Codec getCodec() {
        return null;
      }

    };
  }

  @Override
  protected AsyncRpcClient createRpcClient(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf);
  }

  @Override
  protected AsyncRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf, new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
              @Override
              public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                  throws Exception {
                promise.setFailure(new RuntimeException("Injected fault"));
              }
            });
          }
        });
  }

  @Test
  public void testAsyncConnectionSetup() throws Exception {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
        SERVICE, null)), new InetSocketAddress("localhost", 0), CONF,
        new FifoRpcScheduler(CONF, 1));
    AsyncRpcClient client = createRpcClient(CONF);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();

      RpcChannel channel =
          client.createRpcChannel(ServerName.valueOf(address.getHostName(), address.getPort(),
            System.currentTimeMillis()), User.getCurrent(), 0);

      final AtomicBoolean done = new AtomicBoolean(false);

      channel.callMethod(md, new PayloadCarryingRpcController(), param, md.getOutputType()
          .toProto(), new RpcCallback<Message>() {
        @Override
        public void run(Message parameter) {
          done.set(true);
        }
      });

      TEST_UTIL.waitFor(1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return done.get();
        }
      });
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testRTEDuringAsyncConnectionSetup() throws Exception {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), CONF,
            new FifoRpcScheduler(CONF, 1));
    AsyncRpcClient client = createRpcClientRTEDuringConnectionSetup(CONF);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();

      RpcChannel channel =
          client.createRpcChannel(ServerName.valueOf(address.getHostName(), address.getPort(),
            System.currentTimeMillis()), User.getCurrent(), 0);

      final AtomicBoolean done = new AtomicBoolean(false);

      PayloadCarryingRpcController controller = new PayloadCarryingRpcController();
      controller.notifyOnFail(new RpcCallback<IOException>() {
        @Override
        public void run(IOException e) {
          done.set(true);
          LOG.info("Caught expected exception: " + e.toString());
          assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
        }
      });

      channel.callMethod(md, controller, param, md.getOutputType().toProto(),
        new RpcCallback<Message>() {
          @Override
          public void run(Message parameter) {
            done.set(true);
            fail("Expected an exception to have been thrown!");
          }
        });

      TEST_UTIL.waitFor(1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return done.get();
        }
      });
    } finally {
      client.close();
      rpcServer.stop();
    }
  }
}
