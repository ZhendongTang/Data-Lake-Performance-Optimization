package tools;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class MergePartitionInfoTable {
    /**
     * 合并元数据表partition_info中的多次upsert的快照列表
     * @note
     * step1: 根据table_id读表partition_info
     * <br>step2: 根据version升序排序
     * <br>step3: 按照排序顺序合并多次upsert的快照成一个集合
     * <br>step4: 将合并结果写入partition_info表 version=999
     */
    static public void mergePartitionInfo() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        //step1: 根据table_id读表partition_info
        String sql = String.format(
                "select table_id, partition_desc, version, commit_op, snapshot, expression from partition_info");
        System.out.println(sql);
        List<PartitionInfo> rsList = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                partitionInfo.setCommitOp(rs.getString("commit_op"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
            //step2: 根据version升序排序
            rsList = rsList.stream().sorted(Comparator.comparing(PartitionInfo::getVersion)).collect(Collectors.toList());
            List<UUID> uuidList2 = new ArrayList<>();
            PartitionInfo partitionInfo = new PartitionInfo();
            //按照排序顺序合并多次upsert的快照成一个集合
            for(PartitionInfo i: rsList){
                Collections.addAll(uuidList2, i.getSnapshot().toArray(new UUID[0]));
            }
            partitionInfo.setTableId(rsList.get(0).getTableId());
            partitionInfo.setPartitionDesc(rsList.get(0).getPartitionDesc());
            partitionInfo.setVersion(999);
            partitionInfo.setCommitOp(rsList.get(0).getCommitOp());
            partitionInfo.setSnapshot(uuidList2);
            partitionInfo.setExpression(rsList.get(0).getExpression());
            //step4: 将合并结果写入partition_info表 version=999
            pstmt = conn.prepareStatement("insert into partition_info (table_id, partition_desc, version, " +
                    "commit_op, snapshot, expression) values (?, ?, ?, ? ,?, ?)");
            Array array = conn.createArrayOf("UUID", partitionInfo.getSnapshot().toArray());
            pstmt.setString(1, partitionInfo.getTableId());
            pstmt.setString(2, partitionInfo.getPartitionDesc());
            pstmt.setInt(3, partitionInfo.getVersion());
            pstmt.setString(4, partitionInfo.getCommitOp());
            pstmt.setArray(5, array);
            pstmt.setString(6, partitionInfo.getExpression());
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }

    }

}
