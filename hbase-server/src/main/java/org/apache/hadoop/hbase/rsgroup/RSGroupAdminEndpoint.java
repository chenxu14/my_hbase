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

package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServerToGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServerToGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTableToGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTableToGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RefreshGroupServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RefreshGroupServersResponse;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterObserver, CoprocessorService {
  private static final Log LOG = LogFactory.getLog(RSGroupAdminEndpoint.class);

  private MasterServices master = null;
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private RSGroupInfoManager groupInfoManager;
  private RSGroupAdminServer groupAdminServer;
  private final RSGroupAdminService groupAdminService = new RSGroupAdminServiceImpl();

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    master = ((MasterCoprocessorEnvironment)env).getMasterServices();
    groupInfoManager = new RSGroupInfoManagerImpl(master.getConfiguration(), master.getZooKeeper()); 
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class<?> clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer does not support RegionServer groups.");
    }
    LOG.info("RSGroupAdminEndpoint start successfully");
  }

  @Override
  public Service getService() {
    return groupAdminService;
  }

  RSGroupInfoManager getGroupInfoManager() {
    return groupInfoManager;
  }

  // before preCreateTableHandler, table exits already check 
  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
      HRegionInfo[] regions) throws IOException {
    if(regions != null && regions.length > 0 && regions[0].getTable().isSystemTable()) {
      return;
    }
    String groupName = desc.getValue(HTableDescriptor.GROUP_NAME);
    groupInfoManager.moveTableToGroup(desc.getTableName(), groupName);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
    groupInfoManager.removeTable(tableName);
  }

/**
   * Implementation of RSGroupAdminService defined in RSGroupAdmin.proto.
   * This class calls {@link RSGroupAdminServer} for actual work, converts result to protocol
   * buffer response, handles exceptions if any occurred and then calls the {@code RpcCallback} with
   * the response.
   * Since our CoprocessorHost asks the Coprocessor for a Service
   * ({@link CoprocessorService#getService()}) instead of doing "coproc instanceOf Service"
   * and requiring Coprocessor itself to be Service (something we do with our observers),
   * we can use composition instead of inheritance here. That makes it easy to manage
   * functionalities in concise classes (sometimes inner classes) instead of single class doing
   * many different things.
   */
  private class RSGroupAdminServiceImpl extends RSGroupAdminProtos.RSGroupAdminService {
    @Override
    public void getRSGroupInfo(RpcController controller,
        GetRSGroupInfoRequest request, RpcCallback<GetRSGroupInfoResponse> done) {
      GetRSGroupInfoResponse.Builder builder = GetRSGroupInfoResponse.newBuilder();
      String groupName = request.getRSGroupName();
      try {
        RSGroupInfo rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
        if (rsGroupInfo != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(rsGroupInfo));
        }
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

	@Override
    public void getRSGroupInfoOfTable(RpcController controller,
        GetRSGroupInfoOfTableRequest request, RpcCallback<GetRSGroupInfoOfTableResponse> done) {
      GetRSGroupInfoOfTableResponse.Builder builder = GetRSGroupInfoOfTableResponse.newBuilder();
      try {
        TableName tableName = ProtobufUtil.toTableName(request.getTableName());
        RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupInfoOfTable(tableName);
        if (RSGroupInfo != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
        }
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void balanceRSGroup(RpcController controller,
        BalanceRSGroupRequest request, RpcCallback<BalanceRSGroupResponse> done) {
      BalanceRSGroupResponse.Builder builder = BalanceRSGroupResponse.newBuilder();
      try {
        builder.setBalanceRan(groupAdminServer.balanceRSGroup(request.getRSGroupName()));
      } catch (IOException e) {
        setControllerException(controller, e);
        builder.setBalanceRan(false);
      }
      done.run(builder.build());
    }

    @Override
    public void listRSGroupInfos(RpcController controller,
        ListRSGroupInfosRequest request, RpcCallback<ListRSGroupInfosResponse> done) {
      ListRSGroupInfosResponse.Builder builder = ListRSGroupInfosResponse.newBuilder();
      try {
        for (RSGroupInfo RSGroupInfo : groupAdminServer.listRSGroups()) {
          builder.addRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
        }
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void getRSGroupInfoOfServer(RpcController controller,
        GetRSGroupInfoOfServerRequest request, RpcCallback<GetRSGroupInfoOfServerResponse> done) {
      GetRSGroupInfoOfServerResponse.Builder builder = GetRSGroupInfoOfServerResponse.newBuilder();
      try {
        Address hp = Address.fromParts(request.getServer().getHostName(),
            request.getServer().getPort());
        RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupOfServer(hp);
        if (RSGroupInfo != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
        }
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void refreshGroupServers(RpcController controller, RefreshGroupServersRequest request,
        RpcCallback<RefreshGroupServersResponse> done) {
      RefreshGroupServersResponse.Builder builder = RefreshGroupServersResponse.newBuilder();
      try {
		groupAdminServer.refreshGroupServers();
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
	}

    @Override
    public void moveTableToGroup(RpcController controller, MoveTableToGroupRequest request,
        RpcCallback<MoveTableToGroupResponse> done) {
      MoveTableToGroupResponse.Builder builder = MoveTableToGroupResponse.newBuilder();
      try {
        TableName tableName = ProtobufUtil.toTableName(request.getTableName());
        groupAdminServer.moveTableToGroup(tableName, request.getGroupName());
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void moveServerToGroup(RpcController controller, MoveServerToGroupRequest request,
        RpcCallback<MoveServerToGroupResponse> done) {
      MoveServerToGroupResponse.Builder builder = MoveServerToGroupResponse.newBuilder();
      try {
        Address address = Address.fromParts(request.getServer().getHostName(),
            request.getServer().getPort());
        groupAdminServer.moveServerToGroup(address, request.getGroupName());
      } catch (IOException e) {
        setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    private void setControllerException(RpcController controller, IOException e) {
      if (controller != null) {
        if (controller instanceof org.apache.hadoop.hbase.ipc.ServerRpcController) {
          ((ServerRpcController)controller).setFailedOn(e);
        } else {
          controller.setFailed(StringUtils.stringifyException(e));
        }
      }
    }
  }
}
