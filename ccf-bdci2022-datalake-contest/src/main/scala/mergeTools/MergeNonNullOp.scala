package mergeTools

import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator

class MergeNonNullOp[T] extends MergeOperator[T]{
  override def mergeData(input: Seq[T]): T = {
//    input.filter(ele => !ele.equals("null") && ele != null).last
    var i = input.length - 1
    var lastNonNull = input(i)
    while (lastNonNull.equals("null") && i > 0) {
      i = i - 1
      lastNonNull = input(i)
    }
    lastNonNull
  }
}


