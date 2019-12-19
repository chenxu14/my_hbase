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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServerToGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTableToGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RefreshGroupServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import com.google.protobuf.ServiceException;

/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Private
class RSGroupAdminClient implements RSGroupAdmin {
  private RSGroupAdminService.BlockingInterface stub;

  public RSGroupAdminClient(Connection conn) throws IOException {
    stub = RSGroupAdminService.newBlockingStub(conn.getAdmin().coprocessorService());
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    try {
      GetRSGroupInfoResponse resp = stub.getRSGroupInfo(null,
          GetRSGroupInfoRequest.newBuilder().setRSGroupName(groupName).build());
      if(resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    GetRSGroupInfoOfTableRequest request = GetRSGroupInfoOfTableRequest.newBuilder().setTableName(
        ProtobufUtil.toProtoTableName(tableName)).build();
    try {
      GetRSGroupInfoOfTableResponse resp = stub.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    BalanceRSGroupRequest request = BalanceRSGroupRequest.newBuilder()
        .setRSGroupName(groupName).build();
    try {
      return stub.balanceRSGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    try {
      List<RSGroupProtos.RSGroupInfo> resp = stub.listRSGroupInfos(null,
          ListRSGroupInfosRequest.getDefaultInstance()).getRSGroupInfoList();
      List<RSGroupInfo> result = new ArrayList<>(resp.size());
      for(RSGroupProtos.RSGroupInfo entry : resp) {
        result.add(RSGroupProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    GetRSGroupInfoOfServerRequest request = GetRSGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.ServerName.newBuilder()
                .setHostName(hostPort.getHostname())
                .setPort(hostPort.getPort())
                .build())
            .build();
    try {
      GetRSGroupInfoOfServerResponse resp = stub.getRSGroupInfoOfServer(null, request);
      if (resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void refreshGroupServers() throws IOException {
    RefreshGroupServersRequest request = RefreshGroupServersRequest.newBuilder().build();
	try{
      stub.refreshGroupServers(null, request);
	} catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
	}
  }

  @Override
  public void moveTableToGroup(TableName tableName, String groupName) throws IOException {
    MoveTableToGroupRequest request = MoveTableToGroupRequest.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).setGroupName(groupName).build();
    try{
      stub.moveTableToGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void moveServerToGroup(Address address, String groupName) throws IOException {
    MoveServerToGroupRequest request = MoveServerToGroupRequest.newBuilder()
        .setServer(HBaseProtos.ServerName.newBuilder()
            .setHostName(address.getHostname())
            .setPort(address.getPort()).build())
        .setGroupName(groupName)
        .build();
    try{
      stub.moveServerToGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
