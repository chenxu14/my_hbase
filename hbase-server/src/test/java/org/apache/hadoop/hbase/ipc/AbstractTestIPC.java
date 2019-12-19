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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * Some basic ipc tests.
 */
public abstract class AbstractTestIPC {

  private static final Log LOG = LogFactory.getLog(AbstractTestIPC.class);

  private static byte[] CELL_BYTES = Bytes.toBytes("xyz");
  private static KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);
  protected static final Configuration CONF = HBaseConfiguration.create();
  protected RpcServer createRpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler) throws IOException {
    return RpcServerFactory.createRpcServer(server, name, services, bindAddress, conf, scheduler);
  }

  // We are using the test TestRpcServiceProtos generated classes and Service because they are
  // available and basic with methods like 'echo', and ping. Below we make a blocking service
  // by passing in implementation of blocking interface. We use this service in all tests that
  // follow.
  static final BlockingService SERVICE =
      TestRpcServiceProtos.TestProtobufRpcProto
          .newReflectiveBlockingService(new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

            @Override
            public EmptyResponseProto ping(RpcController controller, EmptyRequestProto request)
                throws ServiceException {
              return EmptyResponseProto.getDefaultInstance();
            }

            @Override
            public EmptyResponseProto error(RpcController controller, EmptyRequestProto request)
                throws ServiceException {
              return EmptyResponseProto.getDefaultInstance();
            }

            @Override
            public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
                throws ServiceException {
              if (controller instanceof PayloadCarryingRpcController) {
                PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) controller;
                // If cells, scan them to check we are able to iterate what we were given and since
                // this is
                // an echo, just put them back on the controller creating a new block. Tests our
                // block
                // building.
                CellScanner cellScanner = pcrc.cellScanner();
                List<Cell> list = null;
                if (cellScanner != null) {
                  list = new ArrayList<Cell>();
                  try {
                    while (cellScanner.advance()) {
                      list.add(cellScanner.current());
                    }
                  } catch (IOException e) {
                    throw new ServiceException(e);
                  }
                }
                cellScanner = CellUtil.createCellScanner(list);
                ((PayloadCarryingRpcController) controller).setCellScanner(cellScanner);
              }
              return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
            }
          });

  protected abstract AbstractRpcClient createRpcClientNoCodec(Configuration conf);

  /**
   * Ensure we do not HAVE TO HAVE a codec.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testNoCodec() throws InterruptedException, IOException {
    Configuration conf = HBaseConfiguration.create();
    AbstractRpcClient client = createRpcClientNoCodec(conf);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), CONF,
        new FifoRpcScheduler(CONF, 1));
    try {
      rpcServer.start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      final String message = "hello";
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      Pair<Message, CellScanner> r =
          client.call(null, md, param, md.getOutputType().toProto(), User.getCurrent(), address,
              new MetricsConnection.CallStats());
      assertTrue(r.getSecond() == null);
      // Silly assertion that the message is in the returned pb.
      assertTrue(r.getFirst().toString().contains(message));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient createRpcClient(Configuration conf);

  /**
   * It is hard to verify the compression is actually happening under the wraps. Hope that if
   * unsupported, we'll get an exception out of some time (meantime, have to trace it manually to
   * confirm that compression is happening down in the client and server).
   * @throws IOException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchMethodException
   */
  @Test
  public void testCompressCellBlock() throws IOException, InterruptedException, SecurityException,
      NoSuchMethodException, ServiceException {
    Configuration conf = new Configuration(HBaseConfiguration.create());
    conf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    List<Cell> cells = new ArrayList<Cell>();
    int count = 3;
    for (int i = 0; i < count; i++) {
      cells.add(CELL);
    }
    AbstractRpcClient client = createRpcClient(conf);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), CONF,
        new FifoRpcScheduler(CONF, 1));

    try {
      rpcServer.start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      PayloadCarryingRpcController pcrc =
          new PayloadCarryingRpcController(CellUtil.createCellScanner(cells));
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      Pair<Message, CellScanner> r =
          client.call(pcrc, md, param, md.getOutputType().toProto(), User.getCurrent(), address,
              new MetricsConnection.CallStats());
      int index = 0;
      while (r.getSecond().advance()) {
        assertTrue(CELL.equals(r.getSecond().current()));
        index++;
      }
      assertEquals(count, index);
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf)
      throws IOException;

  @Test
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), CONF,
        new FifoRpcScheduler(CONF, 1));
    AbstractRpcClient client = createRpcClientRTEDuringConnectionSetup(conf);
    try {
      rpcServer.start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      client.call(null, md, param, null, User.getCurrent(), address,
          new MetricsConnection.CallStats());
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  /** Tests that the rpc scheduler is called when requests arrive. */
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException {
    RpcScheduler scheduler = spy(new FifoRpcScheduler(CONF, 1));
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), CONF, scheduler);
    verify(scheduler).init((RpcScheduler.Context) anyObject());
    AbstractRpcClient client = createRpcClient(CONF);
    try {
      rpcServer.start();
      verify(scheduler).start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      for (int i = 0; i < 10; i++) {
        client.call(new PayloadCarryingRpcController(
            CellUtil.createCellScanner(ImmutableList.<Cell> of(CELL))), md, param,
            md.getOutputType().toProto(), User.getCurrent(), address,
            new MetricsConnection.CallStats());
      }
      verify(scheduler, times(10)).dispatch((CallRunner) anyObject());
    } finally {
      rpcServer.stop();
      verify(scheduler).stop();
    }
  }

  /** Tests that the rpc scheduler is called when requests arrive. */
  @Test
  public void testRpcMaxRequestSize() throws IOException, InterruptedException {
    Configuration conf = new Configuration(CONF);
    conf.setInt(RpcServer.MAX_REQUEST_SIZE, 100);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(
            SERVICE, null)), new InetSocketAddress("localhost", 0), conf,
        new FifoRpcScheduler(conf, 1));
    AbstractRpcClient client = createRpcClient(conf);
    try {
      rpcServer.start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      // set total RPC size bigger than 100 bytes
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello.hello.hello.hello."
          + "hello.hello.hello.hello.hello.hello.hello.hello.hello.hello.hello.hello").build();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      try {
        client.call(new PayloadCarryingRpcController(
          CellUtil.createCellScanner(ImmutableList.<Cell> of(CELL))), md, param,
          md.getOutputType().toProto(), User.getCurrent(), address,
          new MetricsConnection.CallStats());
        fail("RPC should have failed because it exceeds max request size");
      } catch(IOException ex) {
        // pass
      }
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testWrapException() throws Exception {
    AbstractRpcClient client =
        (AbstractRpcClient) RpcClientFactory.createClient(CONF, "AbstractTestIPC");
    final InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);
    assertTrue(client.wrapException(address, new ConnectException()) instanceof ConnectException);
    assertTrue(client.wrapException(address,
      new SocketTimeoutException()) instanceof SocketTimeoutException);
    assertTrue(client.wrapException(address, new ConnectionClosingException(
        "Test AbstractRpcClient#wrapException")) instanceof ConnectionClosingException);
    assertTrue(client
        .wrapException(address, new CallTimeoutException("Test AbstractRpcClient#wrapException"))
        .getCause() instanceof CallTimeoutException);
  }

}
