package org.zyh.study.pq

import java.text.SimpleDateFormat
import java.util.Calendar

import com.yx.sy.core.udf.DealTenantTimeUdf
import com.yx.sy.core.utils.{DateUtils, HbaseUtils, ResourcesUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object DwsImUserWideTbl {
  private val Log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    var ds_dt:String =null
    var ds_dt_h:String =null
    var ds_dt_min:String=null
    var timeType:String=null
    if(args.length==2&&"None"!=String.valueOf(args(0))){
      val tuple = DateUtils.getDateStr(args(0))
      ds_dt=tuple._1
      ds_dt_h = tuple._2
      ds_dt_min=tuple._3
      timeType =args(1)
      println("ds_dt:",ds_dt)
      println("ds_dt_h:",ds_dt_h)
      println("ds_dt_min",ds_dt_min)
      Log.warn(ds_dt+ds_dt_h+ds_dt_min)
    }else if(args.length==1){
      val tuple = DateUtils.getDateStr(args(0))
      ds_dt=tuple._1
      ds_dt_h = tuple._2
      ds_dt_min=tuple._3
      timeType="00"
    }
    else{
      val cal=Calendar.getInstance
      cal.add(Calendar.DATE,-1)
      ds_dt = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      Log.warn("ds_dt=:"+ds_dt)
      timeType="00"
    }
    val sparkBuilder = SparkSession.builder()
    if("local".equals(ResourcesUtils.getEnv("env"))){
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder
      .appName(this.getClass.getName)
      .config("hive.exec.dynamici.partition","true")
      .config("hive.exec.dynamic.partition.mode","nostrick")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.udf.register("dealtenanttimeudf",DealTenantTimeUdf.dealTenantTime _)
    val ds_dt_str=ds_dt.replace("-","")
    val ds_dt_month = ds_dt.replace("-","").substring(0,6)
    println(ds_dt_month)
    val ds_dt_date = new SimpleDateFormat("yyyy-MM-dd").parse(ds_dt)
    val cal03 =Calendar.getInstance()
    cal03.setTime(ds_dt_date)
    cal03.add(Calendar.DATE,-1)
    val last_ds_dt = new SimpleDateFormat("yyyyMMdd").format(cal03.getTime)
    ///最近15天
    val cal_15ds=Calendar.getInstance()
    cal_15ds.setTime(ds_dt_date)
    cal_15ds.add(Calendar.DATE,-15)
    val last_15ds= new SimpleDateFormat("yyyyMMdd").format(cal_15ds.getTime)
    println(last_15ds)
    val cal01 = Calendar.getInstance()
    cal01.setTime(ds_dt_date)
    cal01.add(Calendar.MONTH,-1)
    val last_dt_month=new SimpleDateFormat("yyyyMM").format(cal01.getTime)
    println(last_dt_month)
    //最近一个月
    val last_1ms=new SimpleDateFormat("yyyyMMdd").format(cal01.getTime)
    //最近三个月
    val cal_3ms=Calendar.getInstance()
    cal_3ms.setTime(ds_dt_date)
    cal_3ms.add(Calendar.MONTH,-3)
    val last_3ms=new SimpleDateFormat("yyyyMMdd").format(cal_3ms.getTime)
    //最近四个月
    val cal_4ms=Calendar.getInstance()
    cal_4ms.setTime(ds_dt_date)
    cal_4ms.add(Calendar.MONTH,-4)
    val last_4ms=new SimpleDateFormat("yyyyMMdd").format(cal_4ms.getTime)

    //最近半年
    val cal_6ms=Calendar.getInstance()
    cal_6ms.setTime(ds_dt_date)
    cal_6ms.add(Calendar.MONTH,-6)
    val last_6ms=new SimpleDateFormat("yyyyMMdd").format(cal_6ms.getTime)

    //最近一年
    val cal_1y=Calendar.getInstance()
    cal_1y.setTime(ds_dt_date)
    cal_1y.add(Calendar.YEAR,-1)
    val last_1y=new SimpleDateFormat("yyyyMMdd").format(cal_1y.getTime)

    val cal02 = Calendar.getInstance()
    cal02.setTime(ds_dt_date)
    cal02.add(Calendar.YEAR,-1)
    val last_dt_year= new SimpleDateFormat("yyyy").format(cal02.getTime)
    val ds_dt_year= ds_dt.replace("-","").substring(0,4)

    val tableName="SAAS:DWS_IM_USER_WIDE_TBL"
    val hbaseConf = HBaseConfiguration.create()
    val zookeeperQuorum = ResourcesUtils.getHbasePropValues("ZOOKEEPER_QUORUM")
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,zookeeperQuorum)
    hbaseConf.set("zookeeper.znode.parent","/hbase-unsecure")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    hbaseConf.set(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, "1024")
    //System.setProperty("HADOOP_USER_NAME","hdfs")
    val columnFamily:List[String]=List("ST","EXT","MB")
    HbaseUtils.creatTable(hbaseConf,tableName,columnFamily)
    var hfilePath=ResourcesUtils.getHbasePropValues("HFILEPATH")+"/"+tableName.split(":")(1)
    val hadoopConf = new Configuration()
    val defaultFS = ResourcesUtils.getHadoopPropValues("fs.defaultFS")
    val system = FileSystem.get(hadoopConf)
    val permission:FsPermission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL)
    hadoopConf.set("fs.defaultFS",defaultFS)

    import spark.sql
    //1.会员基本信息
    val member_str=
      s"""
         |select
         | concat(t1.tenant_id,'_',t1.member_id) as ROW_KEY
         |,cast(t1.member_id as String) as MEMBER_ID
         |,cast(t1.tenant_id as String) as TENANT_ID
         |,cast(t1.instance_id as String) AS APP_INSTANCE_ID
         |,cast(t1.member_type as String) AS MEMBER_TYPE
         |,cast(floor(coalesce(((year(current_date)-year(t1.birthday))*12+month(current_date)-month(t1.birthday))/12,0)) as String) AS AGE
         |,t1.channel_code AS CHANNEL_CODE
         |,t1.user_name    AS USER_NAME
         |,t1.nick_name    AS NICK_NAME
         |,t1.real_name    AS REAL_NAME
         |,t1.avatar       AS AVATAR
         |,t1.invite_code  AS INVITE_CODE
         |,t1.invite_name  AS INVITE_NAME
         |,t1.position     AS POSITION
         |,cast(t1.member_status as string) AS MEMBER_STATUS
         |,t1.company       AS COMPANY
         |,t1.address       AS CONTACT_ADDR
         |,t1.province      AS PROVINCE
         |,t1.city          AS CITY
         |,t1.district      AS DISTRICT
         |,t1.phone         AS PHONE_NUMBER
         |,substr(t1.phone,1,3) as PHONE_SEGMENT
         |,t1.email            AS EMAIL
         |,cast(t1.sex as string)  AS GENDER
         |,t1.birthday AS BIRTHDAY
         |,t1.industry          AS INDUSTRY
         |,cast(t1.education_level as String)  AS EDUCATION_LEVEL
         |,cast(t1.marital_status as String) AS MARITAL_STATUS
         |,cast(t1.member_level_define_id as String) as LEVEL
         |,t1.level_name             AS LEVEL_NAME
         |,t1.model_name             AS MODEL_NAME
         |,case
         |   when t1.self_regist_flag=1
         |   then t1.self_regist_time
         |   else null
         | end                    AS REGISTER_TIME
         |,t1.member_no              AS MEMBER_NO
         |,t1.customer_no            AS CUSTOMER_NO
         |,cast(t1.is_pay_member as String)  AS IS_PAY_MEMBER
         |,cast(t1.is_group_buying as String)    AS IS_GROUP_BUYING
         |,t1.member_card_no         AS MEMBER_CARD_NO
         |,t1.refresh_level_time AS REFRESH_LEVEL_TIME
         |,t1.level_expiration_time AS LEVEL_EXPIRATION_TIME
         |,cast(t1.is_premium_membership as String) AS IS_PREMIUM_MEMBERSHIP
         |,cast(t1.store_id as string) AS STORE_ID
         |,t1.store_code            AS STORE_CODE
         |,t1.store_name            AS STORE_NAME
         |,cast(t1.growth_value as string)    AS GROWTH_VALUE
         |,cast(case when t1.register_time is not null
         |           then datediff(from_unixtime(unix_timestamp('$ds_dt_str','yyyyMMdd'),'yyyy-MM-dd hh:mm:ss'),from_unixtime(unix_timestamp(cast(t1.register_time as string),'yyyy-MM-dd'),'yyyy-MM-dd hh:mm:ss'))
         |           else null
         |       end as String) as REGISTER_DAYS
         |,case when t1.phone is not null AND t1.phone!=''
         |       then 'Y'
         |       else 'N'
         |   end as PHONE_FLG
         |,case when t2.member_id is not null
         |      then 'Y'
         |      else 'N'
         |   end as IS_ENTER_WECHAT
         | ,cast(t1.MEMBER_ORG_ID as string) as MEMBER_ORG_ID
         | ,t1.MEMBER_ORG_NAME
         | ,cast(t3.org_id as string) as STORE_ORG_ID
         | ,t3.org_name AS STORE_ORG_NAME
         |from dtsaas.dwb_mbr_member_info t1
         | left join  dtsaas.dwd_cstm_member_channel_df t2
         |   on t1.member_id = t2.member_id
         |  and t1.tenant_id = t2.tenant_id
         |  and t2.channel_no='qiye_weixin'
         | left join dtsaas.dwb_dm_sc_shop t3
         |   on t1.store_code= t3.code
         |  and t1.tenant_id = t3.tenant_id
         |where cast(t1.ds_dt as string)='$ds_dt_str'
         |  and t1.member_status!='3'
         |  and t1.member_no != 'none'
         |  and t1.member_no is not null
         |  and t1.row_key is not null
         |  and t1.time_type='$timeType'
       """.stripMargin
    println(member_str)
    val memberDF=sql(member_str)
    /***
      * 2.会员标识信息
      */
    val member_str02=
      s"""
         |SELECT concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY,
         |       member_flg as MEMBER_FLG
         | FROM dtsaas.dws_user_flg_detail t1
         |where cast(ds_dt as string)='$ds_dt_str'
         |   and time_type='$timeType'
         |   and member_no !='none'
         |   and member_no is not null
       """.stripMargin
    println(member_str02)
    val memberDF02 = sql(member_str02)

    /**
      * 3.订单信息
      */
    val order_str=
      s"""
         |select concat(t1.tenant_id,'_',t1.member_id) as ROW_KEY,
         |       cast(FIRST_ORDER_TIME as string) as FIRST_ORDER_TIME,
         |       cast(LST_ORDER_TIME as string) as LST_ORDER_TIME,
         |       cast(t1.ORDER_NUM as string) as ORDER_NUM,
         |       cast(round(t1.ORDER_AMOUNT,2) as string) as ORDER_AMOUNT,
         |       cast(round(t1.ORDER_DISCOUNT,2) as string) as ORDER_DISCOUNT,
         |       t2.first_shop_id as FIRST_ORDER_SHOPID,
         |       t3.last_shop_id as LST_ORDER_SHOPID,
         |       cast(datediff(from_unixtime(unix_timestamp('${ds_dt_str}','yyyyMMdd'),'yyyy-MM-dd hh:mm:ss'),from_unixtime(unix_timestamp(cast(FIRST_ORDER_TIME as string),'yyyyMMdd'),'yyyy-MM-dd hh:mm:ss')) as String) as FIRST_ORDER_INTRVL,
         |       cast(datediff(from_unixtime(unix_timestamp('${ds_dt_str}','yyyyMMdd'),'yyyy-MM-dd hh:mm:ss'),from_unixtime(unix_timestamp(cast(LST_ORDER_TIME as string),'yyyyMMdd'),'yyyy-MM-dd hh:mm:ss')) as String) as LST_ORDER_INTRVL
         |  from (select tenant_id,
         |               member_id,
         |               min(orderdate) as FIRST_ORDER_TIME ,
         |               max(orderdate) as LST_ORDER_TIME,
         |               sum(order_num) as ORDER_NUM,
         |               sum(pay_amount) as ORDER_AMOUNT,
         |               sum(platform_discount_amount)+sum(shop_discount_amount) as ORDER_DISCOUNT
         |          from dtsaas.dws_catering_tr_extl_order
         |         where time_type='$timeType'
         |           and member_no is not null
         |         group by tenant_id,member_id) t1
         |    left join dtsaas.dws_catering_tr_extl_order t2
         |     on  t1.tenant_id = t2.tenant_id
         |     and t1.member_id = t2.member_id
         |     and t1.FIRST_ORDER_TIME = t2.orderdate
         |     and t2.time_type = '$timeType'
         |     and t2.member_no is not null
         |    left join dtsaas.dws_catering_tr_extl_order t3
         |     on t1.tenant_id=t3.tenant_id
         |     and t1.member_id=t3.member_id
         |     and t1.LST_ORDER_TIME=t3.orderdate
         |     and t3.time_type='$timeType'
         |     and t3.member_no is not null
       """.stripMargin
    println(order_str)
    val order_strDF = sql(order_str)

    /***
      * 3.1 最近1月订单信息
      */
    val order_1ms=
      s"""
         |select concat(tenant_id,'_',member_id) as ROW_KEY,
         |       cast(sum(order_num) as string) as LST_ORDER_NUM_30D,
         |       cast(sum(pay_amount) as string) as LST_ORDER_AMOUNT_30D,
         |       cast(sum(platform_discount_amount) + sum(shop_discount_amount) as string) as LST_ORDER_DISC_30D,
         |       cast(if(sum(if(pay_amount < total_amount, 0, 1)) > 0, sum(if(pay_amount < total_amount, 0, pay_amount)) / sum(if(pay_amount < total_amount, 0, 1)), 0) as string) as FULL_AVERAGE_PRICE_30D
         |  from dtsaas.dws_catering_tr_extl_order
         | where cast(ds_dt as string) >='$last_dt_month'
         |   and cast(orderdate as string) >= '$last_1ms'
         |   and time_type ='$timeType'
         | group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(order_1ms)
    val order_1msDF = sql(order_1ms)

    /***
      * 3.2 最近3月订单信息
      */
    val order_3ms=
      s"""
         |select concat(tenant_id,'_',member_id) as ROW_KEY,
         |       cast(sum(order_num) as string) as LST_ORDER_NUM_90D,
         |       cast(sum(pay_amount) as string) as LST_ORDER_AMOUNT_90D,
         |       cast((sum(platform_discount_amount) + sum(shop_discount_amount)) as string) as LST_ORDER_DISC_90D,
         |       cast(if(sum(if(pay_amount < total_amount, 0, 1)) > 0, sum(if(pay_amount < total_amount, 0, pay_amount)) / sum(if(pay_amount < total_amount, 0, 1)), 0) as string) as  FULL_AVERAGE_PRICE_90D
         |  from dtsaas.dws_catering_tr_extl_order
         | where orderdate >= '$last_3ms'
         |   and time_type ='$timeType'
         | group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(order_3ms)
    val order_3msDF = sql(order_3ms)

    /**
      * 3.3 最近4月订单信息
      */
        val order_4ms=
      s"""
         |select  concat(tenant_id,'_',member_id) as ROW_KEY
         |       ,cast(if(sum(if(pay_amount < total_amount, 0, 1)) > 0, sum(if(pay_amount < total_amount, 0, pay_amount)) / sum(if(pay_amount < total_amount, 0, 1)), 0) as String) as  FULL_AVERAGE_PRICE_120D
         |  from dtsaas.dws_catering_tr_extl_order
         | where cast(orderdate as string) >='$last_4ms'
         |   and time_type ='$timeType'
         | group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(order_4ms)
    val order_4msDF = sql(order_4ms)

    /**
      *3.4 最近半年订单信息
      */
    val order_6ms=
      s"""
         |select
         |concat(tenant_id,'_',member_id) AS ROW_KEY,
         |cast(sum(order_num) as String) as LST_ORDER_NUM_180D,
         |cast(sum(pay_amount) as String) as LST_ORDER_AMOUNT_180D,
         |cast((sum(platform_discount_amount)+sum(shop_discount_amount)) as String) as LST_ORDER_DISC_180D
         |from dtsaas.dws_catering_tr_extl_order
         |where orderdate >= '$last_6ms'
         |  and time_type='$timeType'
         |group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(order_6ms)
    val order_6msDF = sql(order_6ms)

    /***
      * 3.5 最近一年订单信息
      */
    val order_1y=
      s"""
         |select
         | concat(tenant_id,'_',member_id) AS ROW_KEY,
         | cast(sum(order_num) as String) as LST_ORDER_NUM_360D,
         | cast(sum(pay_amount) as String) as LST_ORDER_AMOUNT_360D,
         | cast((sum(platform_discount_amount)+sum(shop_discount_amount)) as String) as LST_ORDER_DISC_360D,
         | cast(IF(SUM(IF(pay_amount < total_amount, 0, 1)) > 0, SUM(IF(pay_amount < total_amount, 0, pay_amount)) / SUM(IF(pay_amount < total_amount, 0, 1)), 0) as string) AS FULL_AVERAGE_PRICE_360D
         |from dtsaas.dws_catering_tr_extl_order
         |where cast(orderdate as string) >= '$last_1y'
         |  and time_type='$timeType'
         |group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(order_1y)
    val order_1yDF = sql(order_1y)

    /**
      * 4.商品明细
      */
    val order_item_360ds=
      s"""
         |SELECT concat(tt.tenant_id,'_',tt.member_id) AS ROW_KEY,
         |       max(CASE WHEN RN =1
         |                THEN item_code
         |                ELSE null
         |            END) AS LST_ORDITEM_ITEMCODE_1ST_360D,
         |       max(CASE WHEN RN = 2
         |                THEN item_code
         |                ELSE null
         |             END) AS LST_ORDITEM_ITEMCODE_2ST_360D,
         |       max(CASE WHEN RN = 3
         |                THEN item_code
         |                 ELSE null
         |             END) AS LST_ORDITEM_ITEMCODE_3ST_360D,
         |     max(CASE WHEN RN =1
         |          THEN name
         |          ELSE null
         |       END) AS LST_ORDITEM_ITEMNAME_1ST_360D,
         |     max(CASE WHEN RN = 2
         |          THEN name
         |          ELSE null
         |       END) AS LST_ORDITEM_ITEMNAME_2ST_360D,
         |     max(CASE WHEN RN = 3
         |          THEN name
         |          ELSE null
         |       END) AS LST_ORDITEM_ITEMNAME_3ST_360D
         |  FROM(
         |     SELECT t1.tenant_id,
         |       t1.member_id,
         |       t1.item_code,
         |       t2.name,
         |       row_number()over(partition by t1.member_id,t1.tenant_id order by t1.item_num desc) AS RN
         |     FROM (
         |         SELECT tenant_id,member_id,item_code,sum(item_num) as item_num
         |           from dtsaas.dws_catering_tr_trade_item
         |          where days=360
         |            and time_type='$timeType'
         |            and item_code !='0'
         |            and item_code is not null
         |          group by tenant_id,member_id,item_code
         |     ) t1
         |     left join dtsaas.dim_it_item t2
         |     on t1.item_code = t2.code and t1.tenant_id=t2.tenant_id
         |     and cast(t2.ds_dt as string)='$ds_dt_str'
         |   ) tt   where  RN <=3
         |  group by concat(tt.tenant_id,'_',tt.member_id)
       """.stripMargin
    println(order_item_360ds)
    val order_item_360dsDF=sql(order_item_360ds)

    /**
      * 5.积分信息
      */
    val point_str=
      s"""
         |select
         |  concat(tenant_id,'_',member_id) AS ROW_KEY,
         |  cast(sum(exchange_points) as String) as EXCHANGE_POINTS,
         |  cast(sum(get_points) as String) as GET_POINTS,
         |  cast(sum(expired_points) as String) as EXPIRED_POINTS,
         |  cast(sum(clear_points) as String) as  CLEAR_POINTS,
         |  cast(sum(yh_points_count) as String) as  YH_POINTS_COUNT,
         |  cast(sum(get_points_count) as String) as GET_POINTS_COUNT
         |  from dtsaas.dws_mbr_member_point_analysis
         | where time_type='$timeType'
         |  group by concat(tenant_id,'_',member_id)
       """.stripMargin
       println(point_str)
    val point_strDF=sql(point_str)

    /**
      * 6.积分活动
      */
    val point_activity=
      s"""
         | select  concat(tenant_id,'_',member_id) AS ROW_KEY,
         |        cast(SUM(points_cnt) as String)  AS ACTIVITIES_POINT_COUNT
         | from dtsaas.dws_mbr_member_get_point_type_analysis
         |where point_change_type='HAOWAN_DECREASE'  --参与游戏抵扣
         |  and time_type ='$timeType'
         | GROUP BY concat(tenant_id,'_',member_id)
       """.stripMargin
     println(point_activity)
    val point_activityDF=sql(point_activity)

    /**
      * 7.1券信息
      */
    val coupon_str=
      s"""
         |select concat(tenant_id,'_',member_id) AS ROW_KEY
         |      ,cast(sum(received_coupon_cnt) as String) as TOTAL_RECEIVED_COUPON
         |      ,cast(sum(used_coupon_cnt) as String) as TOTAL_USED_COUNPON
         |      ,cast(round((case when sum(received_coupon_cnt)=0
         |                        then 0
         |                        else sum(used_coupon_cnt)/sum(received_coupon_cnt)
         |                     end)*100,2) as String) as TOTAL_USED_COUNPON_PERCENT
         |from dtsaas.dws_mb_coupon_receive_use_info
         |where row_key is not null
         | and row_key!=''
         | and time_type='$timeType'
         | group by concat(tenant_id,'_',member_id)
       """.stripMargin
    println(coupon_str)
    val coupon_strDF=sql(coupon_str)

    /***
      * 7.2.最近15天券信息
      */
    val coupon_15ds=
      s"""
         |select
         | concat(tenant_id,'_',member_id) AS ROW_KEY
         |,case when tt.LAST_RECEVIED_COUPON_15D>0
         |     then 'Y'
         |     else 'N'
         | end as LAST_RECEIVED_COUPON_15D_FLG
         |,case when tt.LAST_USED_COUPON_15D>0
         |     then 'Y'
         |     else 'N'
         | end as LAST_USED_COUPON_15D_FLG
         | ,cast(tt.LAST_RECEVIED_COUPON_15D as String) as LAST_RECEVIED_COUPON_15D
         | ,cast(tt.LAST_USED_COUPON_15D as String) as LAST_USED_COUPON_15D
         | ,cast(round((case when tt.LAST_RECEVIED_COUPON_15D=0
         |             then 0
         |             else tt.LAST_USED_COUPON_15D/tt.LAST_USED_COUPON_15D
         |          end)*100,2) as String) as LAST_USED_COUPON_PERCENT_15D
         |from (select tenant_id
         |          ,member_id
         |          ,sum(received_coupon_cnt) as LAST_RECEVIED_COUPON_15D
         |          ,sum(used_coupon_cnt) as LAST_USED_COUPON_15D
         |      from dtsaas.dws_mb_coupon_receive_use_info
         |     where cast(ds_dt as string) >='$last_15ds'
         |       and time_type='$timeType'
         |       and member_no is not null
         |     group by tenant_id
         |             ,member_id)tt
       """.stripMargin
    println(coupon_15ds)
    val coupon_15dsDF=sql(coupon_15ds)

    /***
      * 7.2.有效券信息
      */
    val coupon_1ds=
      s"""
         |select concat(tenant_id,'_',member_id) AS ROW_KEY
         |      ,cast(count(1) as string) as VALID_COUNPON_CNT
         |  from dtsaas.dwb_mk_coupon_receive_use_flow
         | where coupon_status='10'
         |   and to_date(invalid_time)>=to_date('$ds_dt')
         |   and time_type='$timeType'
         |   and member_no is not null
         | group by concat(tenant_id,'_',member_id)

       """.stripMargin
    println(coupon_1ds)
    val coupon_1dsDF=sql(coupon_1ds)

    /**
      * 9 储值信息
      */
    val recharge_str=
      s"""
         |select
         |  concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY,
         |  cast(FIRST_CHARGE_TIME as string) as FIRST_CHARGE_TIME,
         |  cast(LAST_CHARGE_TIME  as string) as LAST_CHARGE_TIME,
         |  cast(DATEDIFF(from_unixtime(unix_timestamp(cast($ds_dt_str as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(LAST_CHARGE_TIME as string),'yyyyMMdd'),'yyyy-MM-dd')) as String) AS LAST_CHARGE_DAYS,
         |  cast(round(t1.TOTAL_CHARGE_AMOUNT,2) as String) as TOTAL_CHARGE_AMOUNT
         |from
         |(
         |  select
         |    tenant_id
         |    ,member_id
         |    ,min(ds_dt) as FIRST_CHARGE_TIME
         |    ,max(ds_dt) as LAST_CHARGE_TIME
         |    ,sum(coalesce(actual_pay_amt,0)+coalesce(back_pay_amt,0)) as TOTAL_CHARGE_AMOUNT
         |  from  dtsaas.dws_fi_member_recharge_info
         |  where member_no is not null
         |    and member_no!=''
         |    and time_type='$timeType'
         |  group by tenant_id
         |    ,member_id
         |) t1
       """.stripMargin
     println(recharge_str)
    val recharge_strDF=sql(recharge_str)

    /**
      * 9.1 最近1个月储值信息
      */
    val recharge_1ms=
      s"""
         |select
         | concat(t1.tenant_id,'_',t1.member_id)  AS ROW_KEY,
         |cast(round(sum(coalesce(actual_pay_amt,0)+coalesce(back_pay_amt,0)),2) as String) as LAST_CHARGE_AMOUNT_30D
         |from dtsaas.dws_fi_member_recharge_info t1
         |where cast(ds_dt as string) >='$last_1ms'
         |  and row_key is not null
         |  and row_key!=''
         |  and time_type='$timeType'
         |group by concat(t1.tenant_id,'_',t1.member_id)
       """.stripMargin
    println(recharge_1ms)
    val recharge_1msDF=sql(recharge_1ms)

    /**
      * 9.2 最近三月的储值信息
      */
    val recharge_3ms=
      s"""
         |select
         | concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY,
         |cast(round(sum(actual_pay_amt)+sum(back_pay_amt),2) as String) as LAST_CHARGE_AMOUNT_90D
         |from dtsaas.dws_fi_member_recharge_info t1
         |where cast(ds_dt as string) >= '$last_3ms'
         |  and row_key is not null
         |  and row_key!=''
         |  and time_type='$timeType'
         |group by  concat(t1.tenant_id,'_',t1.member_id)
       """.stripMargin
    println(recharge_3ms)
    val recharge_3msDF=sql(recharge_3ms)


    /**
      * 9.3 最近6月储值信息
      */
    val recharge_6ms=
      s"""
         |select
         | concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY,
         |cast(round(sum(actual_pay_amt)+sum(back_pay_amt),2) as String) as LAST_CHARGE_AMOUNT_180D
         |from dtsaas.dws_fi_member_recharge_info t1
         |where cast(ds_dt as string)>='$last_6ms'
         |  and row_key is not null
         |  and row_key!=''
         |  and time_type='$timeType'
         |group by  concat(t1.tenant_id,'_',t1.member_id)
       """.stripMargin
    println(recharge_6ms)
    val recharge_6msDF=sql(recharge_6ms)

    /**
      * 9.4最近1年储值信息
      */
    val recharge_1y=
      s"""
         |select
         | concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY,
         |cast(round(sum(actual_pay_amt)+sum(actual_pay_amt),2) as String) as LAST_CHARGE_AMOUNT_360D
         |from  dtsaas.dws_fi_member_recharge_info t1
         |where cast(ds_dt as string) >='$last_1y'
         |  and row_key is not null
         |  and row_key!=''
         |  and time_type='${timeType}'
         |group by  concat(t1.tenant_id,'_',t1.member_id)
       """.stripMargin
    println(recharge_1y)
    val recharge_1yDF = sql(recharge_1y)

    /**
      * 10.公众号消息
      */
      val mp_receive_msg=
        s"""
           |SELECT  concat(t1.tenant_id,'_',t1.member_id) AS ROW_KEY
           |      ,CASE WHEN T2.MEMBER_ID IS NOT NULL
           |            THEN 'Y'
           |            ELSE 'N'
           |         END IS_MP_ACITIVE_USER
           |  from dtsaas.dwb_mbr_member_info t1
           |  left join (select tenant_id
           |                    ,member_id
           |               from dtsaas.dwd_log_mp_receive_message_df
           |              where to_date(create_time)>=date_sub(to_date('$ds_dt'),1)
           |              group by tenant_id
           |                    ,member_id) t2
           |     on t1.tenant_id =t2.tenant_id
           |    and t1.MEMBER_ID =  T2.MEMBER_ID
           | WHERE cast(t1.ds_dt as string)='$ds_dt_str'
           |   and t1.status!=3
         """.stripMargin
    val mp_receive_msgDF=sql(mp_receive_msg)

    //Save Hfiles on HDFS
    val hbaseConn=HbaseUtils.getConn(hbaseConf)
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    val regionlocator =hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    val admin = hbaseConn.getAdmin
    job.getConfiguration.set("mapred.output.dir",hfilePath)
    HFileOutputFormat2.configureIncrementalLoad(job,table,regionlocator)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)

    //memberDF
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(memberDF,"ROW_KEY","ST").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //    //bulkload Hfile to Hbase
    system.setPermission(new Path(hfilePath),permission)
    var listBufferes:ListBuffer[String]=null
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)
    //memberDF02
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(memberDF02,"ROW_KEY","ST").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 3.订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_strDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 3.1 最近1月订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_1msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 3.2 最近三月订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_3msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 3.3 最近四月订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_4msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 3.4 最近半年订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_6msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 3.5 最近1年订单信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_1yDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 4.商品明细
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(order_item_360dsDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 5.积分信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(point_strDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 6.积分活动
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(point_activityDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 7.券信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(coupon_strDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 7.2.最近15天券信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(coupon_15dsDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /***
      * 7.3.有效券信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(coupon_1dsDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 9 储值信息
      */

    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(recharge_strDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 9.1 最近1个月储值信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(recharge_1msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 9.2 最近三月的储值信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(recharge_3msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)
    /**
      * 9.3 最近6月储值信息
      */
    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(recharge_6msDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 9.4最近1年储值信息
      */

    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(recharge_1yDF,"ROW_KEY","EXT").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)

    /**
      * 10 微信公众号活跃用户
      */

    if(system.exists(new Path(hfilePath))){
      system.delete(new Path(hfilePath),true)
    }
    getHFileRDD(mp_receive_msgDF,"ROW_KEY","ST").saveAsNewAPIHadoopDataset(job.getConfiguration)
    //bulkload Hfile to Hbase
    listBufferes  = getAllFilePath(new Path(hfilePath),system)
    listBufferes.foreach(listBuffer=>{
      system.setPermission(new Path(listBuffer),permission)
    })
    bulkLoader.doBulkLoad(new Path(hfilePath),admin,table,regionlocator)
    table.close()
    hbaseConn.close()
    system.close()
    spark.stop()
    spark.close()
  }
  def getHFileRDD(dataDF:DataFrame,row_Key:String,colFamily:String):RDD[(ImmutableBytesWritable, KeyValue)] ={
    //import org.apache.spark.sql.functions.monotonically_increasing_id
    //import org.apache.spark.sql.functions.col
    //import org.apache.spark.sql.functions.concat
    //import org.apache.spark.sql.functions.lit
    //val newDataDF = dataDF.withColumn("ROW_KEY",concat(monotonically_increasing_id,lit("_"),col(rowKey)))
   // val fields=newDataDF.columns.dropWhile(_=="ROW_KEY").dropWhile(_==rowKey).sorted
   val fields=dataDF.columns.dropWhile(_==row_Key).sorted
    println(fields)
    val data = dataDF.rdd.map(row => {
      //var kvlist:Seq[KeyValue] = List()
      var kvlist=new ListBuffer[KeyValue]()
      var rowkey:Array[Byte] =null
      var cn:Array[Byte] =null
      var v:Array[Byte] =null
      var kv: KeyValue = null
      rowkey = Bytes.toBytes(row.getAs[String](row_Key))
      val cf: Array[Byte] = Bytes.toBytes(colFamily)
      var i=0
      for(i<-0 to (fields.length-1)){
        cn = fields(i).getBytes()
        if(StringUtils.isNotEmpty(row.getAs[String](fields(i)))){
          v = Bytes.toBytes(row.getAs[String](fields(i)))
        }else{
          v="".getBytes()
        }
        kv=new KeyValue(rowkey,cf,cn,System.currentTimeMillis()+i,v)
        kvlist.append(kv)
      }
      (new ImmutableBytesWritable(rowkey), kvlist)
    })

    val resultRDD:RDD[(ImmutableBytesWritable,KeyValue)] = data.flatMapValues(s => {
      s.iterator
    }).sortBy(x => (x._1,x._2.getKeyString), true)
    resultRDD
  }
  def deletePath(path:String): Unit ={

  }
  def getAllFilePath(hfilePath:Path,fs:FileSystem):ListBuffer[String]={
      var list= new ListBuffer[String]()
       val filestatuses = fs.listStatus(hfilePath)
       filestatuses.foreach(
         filestatus=>{
           if(filestatus.isDirectory){
             list.append(filestatus.getPath.toString)
             list.appendAll(getAllFilePath(filestatus.getPath,fs))
           }else{
             list.append(filestatus.getPath.toString)
           }
         }
       )
       list
    }
}
