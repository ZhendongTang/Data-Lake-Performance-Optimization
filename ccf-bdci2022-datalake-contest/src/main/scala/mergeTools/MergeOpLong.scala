package mergeTools

import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator

class MergeOpLong extends MergeOperator[Long] {
  override def mergeData(input: Seq[Long]): Long = {
    input.sum
  }
}