package org.zyh.study.dataManager

import org.apache.hadoop.hive.ql.tools.LineageInfo


/**
 * @ Author     ：aresyhzhang
 * @ Date       ：Created in 14:15 2022/1/10
 * @ Description：
 */
object HiveLineageInfoTest extends {
  def main(args: Array[String]): Unit = {
    val info = new LineageInfo()
    info.getLineageInfo("")
  }


}
