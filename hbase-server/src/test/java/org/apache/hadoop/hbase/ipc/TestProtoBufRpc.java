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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in <code>src/test/protobuf/test.proto</code>
 * and protobuf service definition from <code>src/test/protobuf/test_rpc_service.proto</code>
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class TestProtoBufRpc {
  public final static String ADDRESS = "localhost";
  public static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;

  @Parameters(name = "{index}: rpcServerImpl={0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[] { SimpleRpcServer.class.getName() },
        new Object[] { NettyRpcServer.class.getName() });
  }

  @Parameter(0)
  public String rpcServerImpl;

  /**
   * Implementation of the test service defined out in TestRpcServiceProtos
   */
  static class PBServerImpl
  implements TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface {
    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public EmptyResponseProto error(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new IOException("error"));
    }
  }

  @Before
  public  void setUp() throws IOException { // Setup server for both protocols
    this.conf = HBaseConfiguration.create();
    this.conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
        rpcServerImpl);
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer");
    log.setLevel(Level.DEBUG);
    log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer.trace");
    log.setLevel(Level.TRACE);
    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service =
      TestRpcServiceProtos.TestProtobufRpcProto.newReflectiveBlockingService(serverImpl);
    // Get RPC server for server side implementation
    this.server = RpcServerFactory.createRpcServer(null, "testrpc",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
        new InetSocketAddress(ADDRESS, PORT), conf,
        new FifoRpcScheduler(conf, 10));
    InetSocketAddress address = server.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    this.isa = address;
    this.server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test (expected=org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException.class
      /*Thrown when we call stub.error*/)
  public void testProtoBufRpc() throws Exception {
    RpcClient rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(this.isa.getHostName(), this.isa.getPort(), System.currentTimeMillis()),
        User.getCurrent(), 0);
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
        TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(channel);
      // Test ping method
      TestProtos.EmptyRequestProto emptyRequest =
        TestProtos.EmptyRequestProto.newBuilder().build();
      stub.ping(null, emptyRequest);

      // Test echo method
      EchoRequestProto echoRequest = EchoRequestProto.newBuilder().setMessage("hello").build();
      EchoResponseProto echoResponse = stub.echo(null, echoRequest);
      Assert.assertEquals(echoResponse.getMessage(), "hello");

      stub.error(null, emptyRequest);
      fail("Expected exception is not thrown");
    } finally {
      rpcClient.close();
    }
  }
}
