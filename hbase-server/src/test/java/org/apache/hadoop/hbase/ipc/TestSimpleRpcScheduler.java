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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandlerImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category(SmallTests.class)
public class TestSimpleRpcScheduler {
  private static final Log LOG = LogFactory.getLog(TestSimpleRpcScheduler.class);

  private final RpcScheduler.Context CONTEXT = new RpcScheduler.Context() {
    @Override
    public InetSocketAddress getListenerAddress() {
      return InetSocketAddress.createUnresolved("127.0.0.1", 1000);
    }
  };
  private Configuration conf;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testBasic() throws IOException, InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    RpcScheduler scheduler = new SimpleRpcScheduler(
        conf, 10, 0, 0, qosFunction, 0);
    scheduler.init(CONTEXT);
    scheduler.start();
    CallRunner task = createMockTask();
    task.setStatus(new MonitoredRPCHandlerImpl());
    scheduler.dispatch(task);
    verify(task, timeout(1000)).run();
    scheduler.stop();
  }

  @Test
  public void testHandlerIsolation() throws IOException, InterruptedException {
    CallRunner generalTask = createMockTask();
    CallRunner priorityTask = createMockTask();
    CallRunner replicationTask = createMockTask();
    List<CallRunner> tasks = ImmutableList.of(
        generalTask,
        priorityTask,
        replicationTask);
    Map<CallRunner, Integer> qos = ImmutableMap.of(
        generalTask, 0,
        priorityTask, HConstants.HIGH_QOS + 1,
        replicationTask, HConstants.REPLICATION_QOS);
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    final Map<CallRunner, Thread> handlerThreads = Maps.newHashMap();
    final CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
    Answer<Void> answerToRun = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        synchronized (handlerThreads) {
          handlerThreads.put(
              (CallRunner) invocationOnMock.getMock(),
              Thread.currentThread());
        }
        countDownLatch.countDown();
        return null;
      }
    };
    for (CallRunner task : tasks) {
      task.setStatus(new MonitoredRPCHandlerImpl());
      doAnswer(answerToRun).when(task).run();
    }

    RpcScheduler scheduler = new SimpleRpcScheduler(
        conf, 1, 1 ,1, qosFunction, HConstants.HIGH_QOS);
    scheduler.init(CONTEXT);
    scheduler.start();
    for (CallRunner task : tasks) {
      when(qosFunction.getPriority((RPCProtos.RequestHeader) anyObject(),
        (Message) anyObject(), (User) anyObject()))
          .thenReturn(qos.get(task));
      scheduler.dispatch(task);
    }
    for (CallRunner task : tasks) {
      verify(task, timeout(1000)).run();
    }
    scheduler.stop();

    // Tests that these requests are handled by three distinct threads.
    countDownLatch.await();
    assertEquals(3, ImmutableSet.copyOf(handlerThreads.values()).size());
  }

  private CallRunner createMockTask() {
    ServerCall call = mock(ServerCall.class);
    CallRunner task = mock(CallRunner.class);
    when(task.getRpcCall()).thenReturn(call);
    return task;
  }

  @Test
  public void testRpcScheduler() throws Exception {
    testRpcScheduler(SimpleRpcScheduler.CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE);
    testRpcScheduler(SimpleRpcScheduler.CALL_QUEUE_TYPE_FIFO_CONF_VALUE);
  }

  private void testRpcScheduler(final String queueType) throws Exception {
    Configuration schedConf = HBaseConfiguration.create();
    schedConf.set(SimpleRpcScheduler.CALL_QUEUE_TYPE_CONF_KEY, queueType);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RequestHeader.class),
      any(Message.class), any(User.class)))
      .thenReturn(HConstants.NORMAL_QOS);

    RpcScheduler scheduler = new SimpleRpcScheduler(schedConf, 1, 1, 1, priority,
                                                    HConstants.QOS_THRESHOLD);
    try {
      scheduler.start();

      CallRunner smallCallTask = mock(CallRunner.class);
      ServerCall smallCall = mock(ServerCall.class);
      RequestHeader smallHead = RequestHeader.newBuilder().setCallId(1).build();
      when(smallCallTask.getRpcCall()).thenReturn(smallCall);
      when(smallCall.getHeader()).thenReturn(smallHead);

      CallRunner largeCallTask = mock(CallRunner.class);
      ServerCall largeCall = mock(ServerCall.class);
      RequestHeader largeHead = RequestHeader.newBuilder().setCallId(50).build();
      when(largeCallTask.getRpcCall()).thenReturn(largeCall);
      when(largeCall.getHeader()).thenReturn(largeHead);

      CallRunner hugeCallTask = mock(CallRunner.class);
      ServerCall hugeCall = mock(ServerCall.class);
      RequestHeader hugeHead = RequestHeader.newBuilder().setCallId(100).build();
      when(hugeCallTask.getRpcCall()).thenReturn(hugeCall);
      when(hugeCall.getHeader()).thenReturn(hugeHead);

      when(priority.getDeadline(eq(smallHead), any(Message.class))).thenReturn(0L);
      when(priority.getDeadline(eq(largeHead), any(Message.class))).thenReturn(50L);
      when(priority.getDeadline(eq(hugeHead), any(Message.class))).thenReturn(100L);

      final ArrayList<Integer> work = new ArrayList<Integer>();
      doAnswerTaskExecution(smallCallTask, work, 10, 250);
      doAnswerTaskExecution(largeCallTask, work, 50, 250);
      doAnswerTaskExecution(hugeCallTask, work, 100, 250);

      scheduler.dispatch(smallCallTask);
      scheduler.dispatch(smallCallTask);
      scheduler.dispatch(smallCallTask);
      scheduler.dispatch(hugeCallTask);
      scheduler.dispatch(smallCallTask);
      scheduler.dispatch(largeCallTask);
      scheduler.dispatch(smallCallTask);
      scheduler.dispatch(smallCallTask);

      while (work.size() < 8) {
        Threads.sleepWithoutInterrupt(100);
      }

      int seqSum = 0;
      int totalTime = 0;
      for (int i = 0; i < work.size(); ++i) {
        LOG.debug("Request i=" + i + " value=" + work.get(i));
        seqSum += work.get(i);
        totalTime += seqSum;
      }
      LOG.debug("Total Time: " + totalTime);

      // -> [small small small huge small large small small]
      // -> NO REORDER   [10 10 10 100 10 50 10 10] -> 930 (FIFO Queue)
      // -> WITH REORDER [10 10 10 10 10 10 50 100] -> 530 (Deadline Queue)
      if (queueType.equals(SimpleRpcScheduler.CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
        assertEquals(530, totalTime);
      } else /* if (queueType.equals(SimpleRpcScheduler.CALL_QUEUE_TYPE_FIFO_CONF_VALUE)) */ {
        assertEquals(930, totalTime);
      }
    } finally {
      scheduler.stop();
    }
  }

  @Test
  public void testScanQueueWithZeroScanRatio() throws Exception {
    Configuration schedConf = HBaseConfiguration.create();
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 1.0f);
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0f);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RequestHeader.class), any(Message.class),
      any(User.class))).thenReturn(HConstants.NORMAL_QOS);

    RpcScheduler scheduler = new SimpleRpcScheduler(schedConf, 2, 1, 1, priority,
                                                    HConstants.QOS_THRESHOLD);
    assertNotEquals(scheduler, null);
  }

  @Test
  public void testScanQueues() throws Exception {
    Configuration schedConf = HBaseConfiguration.create();
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 1.0f);
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.7f);
    schedConf.setFloat(SimpleRpcScheduler.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.5f);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RequestHeader.class), any(Message.class),
      any(User.class))).thenReturn(HConstants.NORMAL_QOS);

    RpcScheduler scheduler = new SimpleRpcScheduler(schedConf, 3, 1, 1, priority,
                                                    HConstants.QOS_THRESHOLD);
    try {
      scheduler.start();

      CallRunner putCallTask = mock(CallRunner.class);
      ServerCall putCall = mock(ServerCall.class);
      putCall.param = RequestConverter.buildMutateRequest(
          Bytes.toBytes("abc"), new Put(Bytes.toBytes("row")));
      RequestHeader putHead = RequestHeader.newBuilder().setMethodName("mutate").build();
      when(putCallTask.getRpcCall()).thenReturn(putCall);
      when(putCall.getHeader()).thenReturn(putHead);
      when(putCall.getParam()).thenReturn(putCall.param);

      CallRunner getCallTask = mock(CallRunner.class);
      ServerCall getCall = mock(ServerCall.class);
      RequestHeader getHead = RequestHeader.newBuilder().setMethodName("get").build();
      when(getCallTask.getRpcCall()).thenReturn(getCall);
      when(getCall.getHeader()).thenReturn(getHead);

      CallRunner scanCallTask = mock(CallRunner.class);
      ServerCall scanCall = mock(ServerCall.class);
      scanCall.param = ScanRequest.newBuilder().build();
      RequestHeader scanHead = RequestHeader.newBuilder().setMethodName("scan").build();
      when(scanCallTask.getRpcCall()).thenReturn(scanCall);
      when(scanCall.getHeader()).thenReturn(scanHead);
      when(scanCall.getParam()).thenReturn(scanCall.param);

      ArrayList<Integer> work = new ArrayList<Integer>();
      doAnswerTaskExecution(putCallTask, work, 1, 1000);
      doAnswerTaskExecution(getCallTask, work, 2, 1000);
      doAnswerTaskExecution(scanCallTask, work, 3, 1000);

      // There are 3 queues: [puts], [gets], [scans]
      // so the calls will be interleaved
      scheduler.dispatch(putCallTask);
      scheduler.dispatch(putCallTask);
      scheduler.dispatch(putCallTask);
      scheduler.dispatch(getCallTask);
      scheduler.dispatch(getCallTask);
      scheduler.dispatch(getCallTask);
      scheduler.dispatch(scanCallTask);
      scheduler.dispatch(scanCallTask);
      scheduler.dispatch(scanCallTask);

      while (work.size() < 6) {
        Threads.sleepWithoutInterrupt(100);
      }

      for (int i = 0; i < work.size() - 2; i += 3) {
        assertNotEquals(work.get(i + 0), work.get(i + 1));
        assertNotEquals(work.get(i + 0), work.get(i + 2));
        assertNotEquals(work.get(i + 1), work.get(i + 2));
      }
    } finally {
      scheduler.stop();
    }
  }

  private void doAnswerTaskExecution(final CallRunner callTask,
      final ArrayList<Integer> results, final int value, final int sleepInterval) {
    callTask.setStatus(new MonitoredRPCHandlerImpl());
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        synchronized (results) {
          results.add(value);
        }
        Threads.sleepWithoutInterrupt(sleepInterval);
        return null;
      }
    }).when(callTask).run();
  }

  @Test
  public void testSoftAndHardQueueLimits() throws Exception {
    Configuration schedConf = HBaseConfiguration.create();

    schedConf.setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 0);
    schedConf.setInt("hbase.ipc.server.max.callqueue.length", 5);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RequestHeader.class), any(Message.class),
      any(User.class))).thenReturn(HConstants.NORMAL_QOS);
    SimpleRpcScheduler scheduler = new SimpleRpcScheduler(schedConf, 0, 0, 0, priority,
      HConstants.QOS_THRESHOLD);
    try {
      scheduler.start();

      CallRunner putCallTask = mock(CallRunner.class);
      ServerCall putCall = mock(ServerCall.class);
      putCall.param = RequestConverter.buildMutateRequest(
        Bytes.toBytes("abc"), new Put(Bytes.toBytes("row")));
      RequestHeader putHead = RequestHeader.newBuilder().setMethodName("mutate").build();
      when(putCallTask.getRpcCall()).thenReturn(putCall);
      when(putCall.getHeader()).thenReturn(putHead);

      assertTrue(scheduler.dispatch(putCallTask));

      schedConf.setInt("hbase.ipc.server.max.callqueue.length", 0);
      scheduler.onConfigurationChange(schedConf);
      assertFalse(scheduler.dispatch(putCallTask));

      schedConf.setInt("hbase.ipc.server.max.callqueue.length", 1);
      scheduler.onConfigurationChange(schedConf);
      assertTrue(scheduler.dispatch(putCallTask));
    } finally {
      scheduler.stop();
    }
  }

  // FIX. I don't get this test (St.Ack). When I time this test, the minDelay is > 2 * codel delay from the get go.
  // So we are always overloaded. The test below would seem to complete the queuing of all the CallRunners inside
  // the codel check interval. I don't think we are skipping codel checking. Second, I think this test has been
  // broken since HBASE-16089 Add on FastPath for CoDel went in. The thread name we were looking for was the name
  // BEFORE we updated: i.e. "RpcServer.CodelBQ.default.handler". But same patch changed the name of the codel
  // fastpath thread to: new FastPathBalancedQueueRpcExecutor("CodelFPBQ.default", handlerCount, numCallQueues...
  // Codel is hard to test. This test is going to be flakey given it all timer-based. Disabling for now till chat
  // with authors.
  @Ignore @Test
  public void testCoDelScheduling() throws Exception {
    Configuration schedConf = HBaseConfiguration.create();

    schedConf.set(SimpleRpcScheduler.CALL_QUEUE_TYPE_CONF_KEY,
      SimpleRpcScheduler.CALL_QUEUE_TYPE_CODEL_CONF_VALUE);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RPCProtos.RequestHeader.class), any(Message.class),
      any(User.class))).thenReturn(HConstants.NORMAL_QOS);
    SimpleRpcScheduler scheduler =
        new SimpleRpcScheduler(schedConf, 1, 1, 1, priority, HConstants.QOS_THRESHOLD);
    try {
      scheduler.start();

      // calls faster than min delay
      for (int i = 0; i < 100; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(5);
        scheduler.dispatch(cr);
      }
      Thread.sleep(100); // make sure fast calls are handled
      assertEquals("None of these calls should have been discarded", 0,
        scheduler.getNumGeneralCallsDropped());

      // calls slower than min delay, but not individually slow enough to be dropped
      for (int i = 0; i < 20; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(6);
        scheduler.dispatch(cr);
      }

      Thread.sleep(100); // make sure somewhat slow calls are handled
      assertEquals("None of these calls should have been discarded", 0,
        scheduler.getNumGeneralCallsDropped());

      // now slow calls and the ones to be dropped
      for (int i = 0; i < 20; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(12);
        scheduler.dispatch(cr);
      }

      Thread.sleep(100); // make sure somewhat slow calls are handled
      assertTrue("There should have been at least 12 calls dropped",
        scheduler.getNumGeneralCallsDropped() > 12);
    } finally {
      scheduler.stop();
    }
  }

  private CallRunner getMockedCallRunner() throws IOException {
    CallRunner putCallTask = mock(CallRunner.class);
    ServerCall putCall = mock(ServerCall.class);
    putCall.param = RequestConverter.buildMutateRequest(
      Bytes.toBytes("abc"), new Put(Bytes.toBytes("row")));
    RPCProtos.RequestHeader putHead = RPCProtos.RequestHeader.newBuilder().setMethodName("mutate").build();
    when(putCallTask.getRpcCall()).thenReturn(putCall);
    when(putCall.getHeader()).thenReturn(putHead);
    return putCallTask;
  }
}
