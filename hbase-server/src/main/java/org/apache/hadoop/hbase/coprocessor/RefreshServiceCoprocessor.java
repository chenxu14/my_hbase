package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.RefreshProtos.GetUserGroupsRequest;
import org.apache.hadoop.hbase.protobuf.generated.RefreshProtos.GetUserGroupsResponse;
import org.apache.hadoop.hbase.protobuf.generated.RefreshProtos.RefreshService;
import org.apache.hadoop.hbase.protobuf.generated.RefreshProtos.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.hbase.protobuf.generated.RefreshProtos.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.security.Groups;

public class RefreshServiceCoprocessor
    implements Coprocessor, CoprocessorService, SingletonCoprocessorService, RefreshService.Interface {

  public static final Log LOG = LogFactory.getLog(RefreshServiceCoprocessor.class);

  @Override
  public Service getService() {
    return RefreshService.newReflectiveService(this);
  }

  @Override
  public void refreshUserToGroupsMappings(RpcController controller, RefreshUserToGroupsMappingsRequest request,
      RpcCallback<RefreshUserToGroupsMappingsResponse> done) {
    Groups.getUserToGroupsMappingService().refresh();
    done.run(RefreshUserToGroupsMappingsResponse.newBuilder().build());
  }

  @Override
  public void getUserGroups(RpcController controller, GetUserGroupsRequest request,
      RpcCallback<GetUserGroupsResponse> done) {
    String user = request.getUser();
    GetUserGroupsResponse.Builder builder = GetUserGroupsResponse.newBuilder();
    try {
      for (String group : Groups.getUserToGroupsMappingService().getGroups(user)) {
        builder.addGroups(group);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    GetUserGroupsResponse response = builder.build();
    done.run(response);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("RefreshServiceCoprocessor start");
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    LOG.info("RefreshServiceCoprocessor stop");
  }

}