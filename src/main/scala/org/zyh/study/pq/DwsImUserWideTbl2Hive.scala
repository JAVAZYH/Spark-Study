package org.zyh.study.pq

import java.text.SimpleDateFormat
import java.util.Calendar

import com.yx.sy.core.udf.DealTenantTimeUdf
import com.yx.sy.core.utils.{DateUtils, ResourcesUtils}
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object DwsImUserWideTbl2Hive {
  private val Log: Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    var ds_dt: String = null
    var ds_dt_h: String = null
    var ds_dt_min: String = null
    var timeType: String = null
    if (args.length == 2&&"None"!=String.valueOf(args(0))) {
      val tuple = DateUtils.getDateStr(args(0))
      ds_dt = tuple._1
      ds_dt_h = tuple._2
      ds_dt_min = tuple._3
      timeType = args(1)
      println("ds_dt:", ds_dt)
      println("ds_dt_h:", ds_dt_h)
      println("ds_dt_min", ds_dt_min)
      Log.warn(ds_dt + ds_dt_h + ds_dt_min)
    } else if (args.length == 1) {
      val tuple = DateUtils.getDateStr(args(0))
      ds_dt = tuple._1
      ds_dt_h = tuple._2
      ds_dt_min = tuple._3
      timeType = "00"
    }
    else {
      val cal = Calendar.getInstance
      cal.add(Calendar.DATE, -1)
      ds_dt = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      Log.warn("ds_dt=:" + ds_dt)
      timeType = "00"
    }
    val sparkBuilder = SparkSession.builder()
    if ("local".equals(ResourcesUtils.getEnv("env"))) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder
      .appName(this.getClass.getName)
      .config("hive.exec.dynamici.partition","true")
      .config("hive.exec.dynamic.partition.mode","nostrick")
      .enableHiveSupport()
      .getOrCreate()
    spark.udf.register("dealtenanttimeudf", DealTenantTimeUdf.dealTenantTime _)
    val ds_dt_str = ds_dt.replace("-", "")
    val ds_dt_month = ds_dt.replace("-", "").substring(0, 6)
    println(ds_dt_month)
    val ds_dt_year = ds_dt.replace("-", "").substring(0, 4)
    println(ds_dt_year)
    import spark.sql
    def catalog=
      s"""
         |{
         | "table":{"namespace":"SAAS","name":"DWS_IM_USER_WIDE_TBL"},
         | "rowkey":"key",
         | "columns":{
         | "ROW_KEY":{"cf":"rowkey", "col":"key", "type":"string"},
         | "MEMBER_ID":{"cf":"ST","col":"MEMBER_ID","type":"string"},
         | "TENANT_ID":{"cf":"ST","col":"TENANT_ID","type":"string"},
         | "APP_INSTANCE_ID":{"cf":"ST","col":"APP_INSTANCE_ID","type":"string"},
         | "MEMBER_TYPE":{"cf":"ST","col":"MEMBER_TYPE","type":"string"},
         | "AGE":{"cf":"ST","col":"AGE","type":"string"},
         | "CHANNEL_CODE":{"cf":"ST","col":"CHANNEL_CODE","type":"string"},
         | "USER_NAME":{"cf":"ST","col":"USER_NAME","type":"string"},
         | "NICK_NAME":{"cf":"ST","col":"NICK_NAME","type":"string"},
         | "REAL_NAME":{"cf":"ST","col":"REAL_NAME","type":"string"},
         | "AVATAR":{"cf":"ST","col":"AVATAR","type":"string"},
         | "INVITE_CODE":{"cf":"ST","col":"INVITE_CODE","type":"string"},
         | "INVITE_NAME":{"cf":"ST","col":"INVITE_NAME","type":"string"},
         | "POSITION":{"cf":"ST","col":"POSITION","type":"string"},
         | "MEMBER_STATUS":{"cf":"ST","col":"MEMBER_STATUS","type":"string"},
         | "COMPANY":{"cf":"ST","col":"COMPANY","type":"string"},
         | "CONTACT_ADDR":{"cf":"ST","col":"CONTACT_ADDR","type":"string"},
         | "PROVINCE":{"cf":"ST","col":"PROVINCE","type":"string"},
         | "CITY":{"cf":"ST","col":"CITY","type":"string"},
         | "DISTRICT":{"cf":"ST","col":"DISTRICT","type":"string"},
         | "PHONE_NUMBER":{"cf":"ST","col":"PHONE_NUMBER","type":"string"},
         | "PHONE_SEGMENT":{"cf":"ST","col":"PHONE_SEGMENT","type":"string"},
         | "EMAIL":{"cf":"ST","col":"EMAIL","type":"string"},
         | "EMAIL_TYPE":{"cf":"ST","col":"EMAIL_TYPE","type":"string"},
         | "GENDER":{"cf":"ST","col":"GENDER","type":"string"},
         | "BIRTHDAY":{"cf":"ST","col":"BIRTHDAY","type":"string"},
         | "INDUSTRY":{"cf":"ST","col":"INDUSTRY","type":"string"},
         | "EDUCATION_LEVEL":{"cf":"ST","col":"EDUCATION_LEVEL","type":"string"},
         | "MARITAL_STATUS":{"cf":"ST","col":"MARITAL_STATUS","type":"string"},
         | "LEVEL":{"cf":"ST","col":"LEVEL","type":"string"},
         | "OPEN_ID":{"cf":"ST","col":"OPEN_ID","type":"string"},
         | "UNION_ID":{"cf":"ST","col":"UNION_ID","type":"string"},
         | "CHANNEL_NAME":{"cf":"ST","col":"CHANNEL_NAME","type":"string"},
         | "CHANNEL":{"cf":"ST","col":"CHANNEL","type":"string"},
         | "TAOBAO_MEMBER_CODE":{"cf":"ST","col":"TAOBAO_MEMBER_CODE","type":"string"},
         | "MODEL_NAME":{"cf":"ST","col":"MODEL_NAME","type":"string"},
         | "REGISTER_TIME":{"cf":"ST","col":"REGISTER_TIME","type":"string"},
         | "REGISTER_DAYS":{"cf":"ST","col":"REGISTER_DAYS","type":"string"},
         | "MEMBER_NO":{"cf":"ST","col":"MEMBER_NO","type":"string"},
         | "CUSTOMER_NO":{"cf":"ST","col":"CUSTOMER_NO","type":"string"},
         | "IS_PAY_MEMBER":{"cf":"ST","col":"IS_PAY_MEMBER","type":"string"},
         | "IS_GROUP_BUYING":{"cf":"ST","col":"IS_GROUP_BUYING","type":"string"},
         | "MEMBER_CARD_NO":{"cf":"ST","col":"MEMBER_CARD_NO","type":"string"},
         | "REFRESH_LEVEL_TIME":{"cf":"ST","col":"REFRESH_LEVEL_TIME","type":"string"},
         | "LEVEL_EXPIRATION_TIME":{"cf":"ST","col":"LEVEL_EXPIRATION_TIME","type":"string"},
         | "IS_PREMIUM_MEMBERSHIP":{"cf":"ST","col":"IS_PREMIUM_MEMBERSHIP","type":"string"},
         | "POINTS_ACCOUNT_CODE":{"cf":"ST","col":"POINTS_ACCOUNT_CODE","type":"string"},
         | "AVAILABLE_POINTS":{"cf":"ST","col":"AVAILABLE_POINTS","type":"string"},
         | "ACCOUNT_TYPE":{"cf":"ST","col":"ACCOUNT_TYPE","type":"string"},
         | "OUTER_ID":{"cf":"ST","col":"OUTER_ID","type":"string"},
         | "USER_TYPE":{"cf":"ST","col":"USER_TYPE","type":"string"},
         | "ACCT_STATUS":{"cf":"ST","col":"ACCT_STATUS","type":"string"},
         | "BALANCE":{"cf":"ST","col":"BALANCE","type":"string"},
         | "FIRST_CHARGE_TIME":{"cf":"EXT","col":"FIRST_CHARGE_TIME","type":"string"},
         | "LAST_CHARGE_TIME":{"cf":"EXT","col":"LAST_CHARGE_TIME","type":"string"},
         | "LAST_CHARGE_DAYS":{"cf":"EXT","col":"LAST_CHARGE_DAYS","type":"string"},
         | "LAST_CHARGE_AMOUNT_30D":{"cf":"EXT","col":"LAST_CHARGE_AMOUNT_30D","type":"string"},
         | "LAST_CHARGE_AMOUNT_90D":{"cf":"EXT","col":"LAST_CHARGE_AMOUNT_90D","type":"string"},
         | "LAST_CHARGE_AMOUNT_120D":{"cf":"EXT","col":"LAST_CHARGE_AMOUNT_120D","type":"string"},
         | "LAST_CHARGE_AMOUNT_180D":{"cf":"EXT","col":"LAST_CHARGE_AMOUNT_180D","type":"string"},
         | "LAST_CHARGE_AMOUNT_360D":{"cf":"EXT","col":"LAST_CHARGE_AMOUNT_360D","type":"string"},
         | "TOTAL_CHARGE_AMOUNT":{"cf":"EXT","col":"TOTAL_CHARGE_AMOUNT","type":"string"},
         | "GROWTH_VALUE":{"cf":"ST","col":"GROWTH_VALUE","type":"string"},
         | "STORE_ID":{"cf":"ST","col":"STORE_ID","type":"string"},
         | "STORE_CODE":{"cf":"ST","col":"STORE_CODE","type":"string"},
         | "STORE_NAME":{"cf":"ST","col":"STORE_NAME","type":"string"},
         | "FIRST_ORDER_TIME":{"cf":"EXT","col":"FIRST_ORDER_TIME","type":"string"},
         | "FIRST_ORDER_SHOPID":{"cf":"EXT","col":"FIRST_ORDER_SHOPID","type":"string"},
         | "FIRST_ORDER_SHOPNAME":{"cf":"EXT","col":"FIRST_ORDER_SHOPNAME","type":"string"},
         | "FIRST_ORDER_INTRVL":{"cf":"EXT","col":"FIRST_ORDER_INTRVL","type":"string"},
         | "LST_ORDER_TIME":{"cf":"EXT","col":"LST_ORDER_TIME","type":"string"},
         | "LST_ORDER_INTRVL":{"cf":"EXT","col":"LST_ORDER_INTRVL","type":"string"},
         | "LST_ORDER_SHOPID":{"cf":"EXT","col":"LST_ORDER_SHOPID","type":"string"},
         | "LST_ORDER_SHOPNAME":{"cf":"EXT","col":"LST_ORDER_SHOPNAME","type":"string"},
         | "LST_ORDER_NUM_30D":{"cf":"EXT","col":"LST_ORDER_NUM_30D","type":"string"},
         | "LST_ORDER_NUM_90D":{"cf":"EXT","col":"LST_ORDER_NUM_90D","type":"string"},
         | "LST_ORDER_NUM_180D":{"cf":"EXT","col":"LST_ORDER_NUM_180D","type":"string"},
         | "LST_ORDER_NUM_360D":{"cf":"EXT","col":"LST_ORDER_NUM_360D","type":"string"},
         | "LST_ORDER_AMOUNT_30D":{"cf":"EXT","col":"LST_ORDER_AMOUNT_30D","type":"string"},
         | "LST_ORDER_AMOUNT_90D":{"cf":"EXT","col":"LST_ORDER_AMOUNT_90D","type":"string"},
         | "LST_ORDER_AMOUNT_180D":{"cf":"EXT","col":"LST_ORDER_AMOUNT_180D","type":"string"},
         | "LST_ORDER_AMOUNT_360D":{"cf":"EXT","col":"LST_ORDER_AMOUNT_360D","type":"string"},
         | "LST_ORDER_DISC_30D":{"cf":"EXT","col":"LST_ORDER_DISC_30D","type":"string"},
         | "LST_ORDER_DISC_90D":{"cf":"EXT","col":"LST_ORDER_DISC_90D","type":"string"},
         | "LST_ORDER_DISC_180D":{"cf":"EXT","col":"LST_ORDER_DISC_180D","type":"string"},
         | "LST_ORDER_DISC_360D":{"cf":"EXT","col":"LST_ORDER_DISC_360D","type":"string"},
         | "FULL_AVERAGE_PRICE_30D":{"cf":"EXT","col":"FULL_AVERAGE_PRICE_30D","type":"string"},
         | "FULL_AVERAGE_PRICE_90D":{"cf":"EXT","col":"FULL_AVERAGE_PRICE_90D","type":"string"},
         | "FULL_AVERAGE_PRICE_120D":{"cf":"EXT","col":"FULL_AVERAGE_PRICE_120D","type":"string"},
         | "FULL_AVERAGE_PRICE_360D":{"cf":"EXT","col":"FULL_AVERAGE_PRICE_360D","type":"string"},
         | "ORDER_NUM":{"cf":"EXT","col":"ORDER_NUM","type":"string"},
         | "ORDER_AMOUNT":{"cf":"EXT","col":"ORDER_AMOUNT","type":"string"},
         | "ORDER_DISCOUNT":{"cf":"EXT","col":"ORDER_DISCOUNT","type":"string"},
         | "DELIVERY_PROVINCE_1ST":{"cf":"EXT","col":"DELIVERY_PROVINCE_1ST","type":"string"},
         | "DELIVERY_PROVINCE_2ND":{"cf":"EXT","col":"DELIVERY_PROVINCE_2ND","type":"string"},
         | "DELIVERY_PROVINCE_3RD":{"cf":"EXT","col":"DELIVERY_PROVINCE_3RD","type":"string"},
         | "DELIVERY_CITY_1ST":{"cf":"EXT","col":"DELIVERY_CITY_1ST","type":"string"},
         | "DELIVERY_CITY_2ND":{"cf":"EXT","col":"DELIVERY_CITY_2ND","type":"string"},
         | "DELIVERY_CITY_3RD":{"cf":"EXT","col":"DELIVERY_CITY_3RD","type":"string"},
         | "TRADE_CHANNEL_1ST":{"cf":"EXT","col":"TRADE_CHANNEL_1ST","type":"string"},
         | "TRADE_CHANNEL_2ND":{"cf":"EXT","col":"TRADE_CHANNEL_2ND","type":"string"},
         | "TRADE_CHANNEL_3RD":{"cf":"EXT","col":"TRADE_CHANNEL_3RD","type":"string"},
         | "TRADE_PERCENT_1ST":{"cf":"EXT","col":"TRADE_PERCENT_1ST","type":"string"},
         | "TRADE_PERCENT_2ND":{"cf":"EXT","col":"TRADE_PERCENT_2ND","type":"string"},
         | "TRADE_PERCENT_3RD":{"cf":"EXT","col":"TRADE_PERCENT_3RD","type":"string"},
         | "PHONE_FLG":{"cf":"ST","col":"PHONE_FLG","type":"string"},
         | "LAST_RECEIVED_COUPON_15D_FLG":{"cf":"EXT","col":"LAST_RECEIVED_COUPON_15D_FLG","type":"string"},
         | "LAST_USED_COUPON_15D_FLG":{"cf":"EXT","col":"LAST_USED_COUPON_15D_FLG","type":"string"},
         | "LAST_RECEVIED_COUPON_15D":{"cf":"EXT","col":"LAST_RECEVIED_COUPON_15D","type":"string"},
         | "LAST_USED_COUPON_15D":{"cf":"EXT","col":"LAST_USED_COUPON_15D","type":"string"},
         | "LAST_USED_COUPON_PERCENT_15D":{"cf":"EXT","col":"LAST_USED_COUPON_PERCENT_15D","type":"string"},
         | "TOTAL_RECEIVED_COUPON":{"cf":"EXT","col":"TOTAL_RECEIVED_COUPON","type":"string"},
         | "TOTAL_USED_COUNPON":{"cf":"EXT","col":"TOTAL_USED_COUNPON","type":"string"},
         | "TOTAL_USED_COUNPON_PERCENT":{"cf":"EXT","col":"TOTAL_USED_COUNPON_PERCENT","type":"string"},
         | "LST_ORDITEM_ITEMCODE_1ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMCODE_1ST_360D","type":"string"},
         | "LST_ORDITEM_ITEMCODE_2ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMCODE_2ST_360D","type":"string"},
         | "LST_ORDITEM_ITEMCODE_3ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMCODE_3ST_360D","type":"string"},
         | "LST_ORDITEM_ITEMNAME_1ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMNAME_1ST_360D","type":"string"},
         | "LST_ORDITEM_ITEMNAME_2ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMNAME_2ST_360D","type":"string"},
         | "LST_ORDITEM_ITEMNAME_3ST_360D":{"cf":"EXT","col":"LST_ORDITEM_ITEMNAME_3ST_360D","type":"string"},
         | "MEMBER_FLG":{"cf":"ST","col":"MEMBER_FLG","type":"string"},
         | "MEMBER_LEVEL_DEFINE_ID":{"cf":"ST","col":"MEMBER_LEVEL_DEFINE_ID","type":"string"},
         | "LEVEL_NAME":{"cf":"ST","col":"LEVEL_NAME","type":"string"},
         | "EXCHANGE_POINTS":{"cf":"EXT","col":"EXCHANGE_POINTS","type":"string"},
         | "YH_POINTS_COUNT":{"cf":"EXT","col":"YH_POINTS_COUNT","type":"string"},
         | "GET_POINTS":{"cf":"EXT","col":"GET_POINTS","type":"string"},
         | "GET_POINTS_COUNT":{"cf":"EXT","col":"GET_POINTS_COUNT","type":"string"},
         | "EXPIRED_POINTS":{"cf":"EXT","col":"EXPIRED_POINTS","type":"string"},
         | "CLEAR_POINTS":{"cf":"EXT","col":"CLEAR_POINTS","type":"string"},
         | "ACTIVITIES_POINT_COUNT":{"cf":"EXT","col":"ACTIVITIES_POINT_COUNT","type":"string"},
         | "IS_ENTER_WECHAT":{"cf":"ST","col":"IS_ENTER_WECHAT","type":"string"},
         | "MEMBER_ORG_ID":{"cf":"ST","col":"MEMBER_ORG_ID","type":"string"},
         | "MEMBER_ORG_NAME":{"cf":"ST","col":"MEMBER_ORG_NAME","type":"string"},
         | "STORE_ORG_ID":{"cf":"ST","col":"STORE_ORG_ID","type":"string"},
         | "STORE_ORG_NAME":{"cf":"ST","col":"STORE_ORG_NAME","type":"string"},
         | "IS_MP_ACITIVE_USER":{"cf":"ST","col":"IS_MP_ACITIVE_USER","type":"string"},
         | "VALID_COUNPON_CNT":{"cf":"EXT","col":"VALID_COUNPON_CNT","type":"string"}
         | }
         |}
       """.stripMargin

    import org.apache.spark.sql.execution.datasources.hbase._
    val zookeeperQuorum = ResourcesUtils.getHbasePropValues("ZOOKEEPER_QUORUM")
    def withCatalog(cat: String): DataFrame = {
      spark
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat,
          "hbase.zookeeper.quorum"->zookeeperQuorum,
          "zookeeper.znode.parent"->"/hbase-unsecure"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }
    val df = withCatalog(catalog)
    print("data show start")
    df.show(200)
    df.printSchema()
    df.createGlobalTempView("tmp_dws_im_user_wide_tbl")
    val dws_im_user_wide_tbl_str=
      s"""
         |INSERT OVERWRITE TABLE DTSAAS.DWS_IM_USER_WIDE_TBL
         |SELECT ROW_KEY
         |      ,MEMBER_ID
         |      ,TENANT_ID
         |      ,APP_INSTANCE_ID
         |      ,cast(MEMBER_TYPE as TINYINT) AS MEMBER_TYPE
         |      ,AGE
         |      ,CHANNEL_CODE
         |      ,USER_NAME
         |      ,NICK_NAME
         |      ,REAL_NAME
         |      ,AVATAR
         |      ,INVITE_CODE
         |      ,INVITE_NAME
         |      ,POSITION
         |      ,MEMBER_STATUS
         |      ,COMPANY
         |      ,CONTACT_ADDR
         |      ,PROVINCE
         |      ,CITY
         |      ,DISTRICT
         |      ,PHONE_NUMBER
         |      ,PHONE_SEGMENT
         |      ,EMAIL
         |      ,EMAIL_TYPE
         |      ,GENDER
         |      ,TO_DATE(BIRTHDAY) AS BIRTHDAY
         |      ,INDUSTRY
         |      ,CAST(EDUCATION_LEVEL AS TINYINT) AS EDUCATION_LEVEL
         |      ,CAST(MARITAL_STATUS AS TINYINT) AS MARITAL_STATUS
         |      ,LEVEL
         |      ,OPEN_ID
         |      ,UNION_ID
         |      ,CHANNEL_NAME
         |      ,CHANNEL
         |      ,TAOBAO_MEMBER_CODE
         |      ,MODEL_NAME
         |      ,REGISTER_TIME
         |      ,CAST(REGISTER_DAYS AS INT) AS REGISTER_DAYS
         |      ,MEMBER_NO
         |      ,CUSTOMER_NO
         |      ,CAST(IS_PAY_MEMBER AS INT) AS IS_PAY_MEMBER
         |      ,CAST(IS_GROUP_BUYING AS INT) AS IS_GROUP_BUYING
         |      ,MEMBER_CARD_NO
         |      ,TO_DATE(REFRESH_LEVEL_TIME) AS REFRESH_LEVEL_TIME
         |      ,TO_DATE(LEVEL_EXPIRATION_TIME) AS LEVEL_EXPIRATION_TIME
         |      ,CAST(IS_PREMIUM_MEMBERSHIP AS TINYINT) AS IS_PREMIUM_MEMBERSHIP
         |      ,POINTS_ACCOUNT_CODE
         |      ,AVAILABLE_POINTS
         |      ,ACCOUNT_TYPE
         |      ,OUTER_ID
         |      ,USER_TYPE
         |      ,ACCT_STATUS
         |      ,CAST(BALANCE AS DOUBLE) AS BALANCE
         |      ,from_unixtime(unix_timestamp(cast(FIRST_CHARGE_TIME as string),'yyyyMMdd'),'yyyy-MM-dd') as FIRST_CHARGE_TIME
         |      ,from_unixtime(unix_timestamp(cast(LAST_CHARGE_TIME as string),'yyyyMMdd'),'yyyy-MM-dd') as LAST_CHARGE_TIME
         |      ,CAST(LAST_CHARGE_DAYS AS INT) AS LAST_CHARGE_DAYS
         |      ,CAST(COALESCE(LAST_CHARGE_AMOUNT_30D,0) AS DOUBLE) AS LAST_CHARGE_AMOUNT_30D
         |      ,CAST(COALESCE(LAST_CHARGE_AMOUNT_90D,0) AS DOUBLE) AS LAST_CHARGE_AMOUNT_90D
         |      ,CAST(COALESCE(LAST_CHARGE_AMOUNT_120D,0) AS DOUBLE) AS LAST_CHARGE_AMOUNT_120D
         |      ,CAST(COALESCE(LAST_CHARGE_AMOUNT_180D,0) AS DOUBLE) AS LAST_CHARGE_AMOUNT_180D
         |      ,CAST(COALESCE(LAST_CHARGE_AMOUNT_360D,0) AS DOUBLE) AS LAST_CHARGE_AMOUNT_360D
         |      ,CAST(COALESCE(TOTAL_CHARGE_AMOUNT,0) AS DOUBLE) AS TOTAL_CHARGE_AMOUNT
         |      ,CAST(GROWTH_VALUE AS INT) AS GROWTH_VALUE
         |      ,STORE_ID
         |      ,STORE_CODE
         |      ,STORE_NAME
         |      ,from_unixtime(unix_timestamp(cast(FIRST_ORDER_TIME as string),'yyyyMMdd'),'yyyy-MM-dd') as FIRST_ORDER_TIME
         |      ,FIRST_ORDER_SHOPID
         |      ,FIRST_ORDER_SHOPNAME
         |      ,CAST(FIRST_ORDER_INTRVL AS INT) AS FIRST_ORDER_INTRVL
         |      ,from_unixtime(unix_timestamp(cast(LST_ORDER_TIME as string),'yyyyMMdd'),'yyyy-MM-dd') as LST_ORDER_TIME
         |      ,CAST(LST_ORDER_INTRVL AS INT) AS LST_ORDER_INTRVL
         |      ,LST_ORDER_SHOPID
         |      ,LST_ORDER_SHOPNAME
         |      ,CAST(COALESCE(LST_ORDER_NUM_30D,0) AS INT) AS LST_ORDER_NUM_30D
         |      ,CAST(COALESCE(LST_ORDER_NUM_90D,0) AS INT) AS LST_ORDER_NUM_90D
         |      ,CAST(COALESCE(LST_ORDER_NUM_180D,0) AS INT) AS LST_ORDER_NUM_180D
         |      ,CAST(COALESCE(LST_ORDER_NUM_360D,0) AS INT) AS LST_ORDER_NUM_360D
         |      ,CAST(COALESCE(LST_ORDER_AMOUNT_30D,0) AS DOUBLE)  AS LST_ORDER_AMOUNT_30D
         |      ,CAST(COALESCE(LST_ORDER_AMOUNT_90D,0) AS DOUBLE) AS LST_ORDER_AMOUNT_90D
         |      ,CAST(COALESCE(LST_ORDER_AMOUNT_180D,0) AS DOUBLE) AS  LST_ORDER_AMOUNT_180D
         |      ,CAST(COALESCE(LST_ORDER_AMOUNT_360D,0) AS DOUBLE) AS LST_ORDER_AMOUNT_360D
         |      ,CAST(COALESCE(LST_ORDER_DISC_30D,0) AS DOUBLE) AS LST_ORDER_DISC_30D
         |      ,CAST(COALESCE(LST_ORDER_DISC_90D,0) AS DOUBLE) AS LST_ORDER_DISC_90D
         |      ,CAST(COALESCE(LST_ORDER_DISC_180D,0) AS DOUBLE) AS LST_ORDER_DISC_180D
         |      ,CAST(COALESCE(LST_ORDER_DISC_360D,0) AS DOUBLE) AS LST_ORDER_DISC_360D
         |      ,CAST(COALESCE(FULL_AVERAGE_PRICE_30D,0) AS DOUBLE) AS FULL_AVERAGE_PRICE_30D
         |      ,CAST(COALESCE(FULL_AVERAGE_PRICE_90D,0) AS DOUBLE) AS FULL_AVERAGE_PRICE_90D
         |      ,CAST(COALESCE(FULL_AVERAGE_PRICE_120D,0) AS DOUBLE) AS FULL_AVERAGE_PRICE_120D
         |      ,CAST(COALESCE(FULL_AVERAGE_PRICE_360D,0) AS DOUBLE) AS FULL_AVERAGE_PRICE_360D
         |      ,CAST(COALESCE(ORDER_NUM,0) AS INT) AS ORDER_NUM
         |      ,CAST(COALESCE(ORDER_AMOUNT,0) AS DOUBLE) AS ORDER_AMOUNT
         |      ,CAST(COALESCE(ORDER_DISCOUNT,0) AS DOUBLE) AS ORDER_DISCOUNT
         |      ,DELIVERY_PROVINCE_1ST
         |      ,DELIVERY_PROVINCE_2ND
         |      ,DELIVERY_PROVINCE_3RD
         |      ,DELIVERY_CITY_1ST
         |      ,DELIVERY_CITY_2ND
         |      ,DELIVERY_CITY_3RD
         |      ,TRADE_CHANNEL_1ST
         |      ,TRADE_CHANNEL_2ND
         |      ,TRADE_CHANNEL_3RD
         |      ,CAST(COALESCE(TRADE_PERCENT_1ST,0) AS DOUBLE) AS TRADE_PERCENT_1ST
         |      ,CAST(COALESCE(TRADE_PERCENT_2ND,0) AS DOUBLE) AS TRADE_PERCENT_2ND
         |      ,CAST(COALESCE(TRADE_PERCENT_3RD,0) AS DOUBLE) AS TRADE_PERCENT_3RD
         |      ,PHONE_FLG
         |      ,LAST_RECEIVED_COUPON_15D_FLG
         |      ,LAST_USED_COUPON_15D_FLG
         |      ,CAST(COALESCE(LAST_RECEVIED_COUPON_15D,0) AS INT) AS LAST_RECEVIED_COUPON_15D
         |      ,CAST(COALESCE(LAST_USED_COUPON_15D,0) AS INT) AS COALESCE9LAST_USED_COUPON_15D
         |      ,CAST(COALESCE(LAST_USED_COUPON_PERCENT_15D,0) AS DOUBLE) AS LAST_USED_COUPON_PERCENT_15D
         |      ,CAST(COALESCE(TOTAL_RECEIVED_COUPON,0) AS INT) AS TOTAL_RECEIVED_COUPON
         |      ,CAST(COALESCE(TOTAL_USED_COUNPON,0) AS INT) AS TOTAL_USED_COUNPON
         |      ,CAST(COALESCE(TOTAL_USED_COUNPON_PERCENT,0) AS DOUBLE) AS TOTAL_USED_COUNPON_PERCENT
         |      ,LST_ORDITEM_ITEMCODE_1ST_360D
         |      ,LST_ORDITEM_ITEMCODE_2ST_360D
         |      ,LST_ORDITEM_ITEMCODE_3ST_360D
         |      ,LST_ORDITEM_ITEMNAME_1ST_360D
         |      ,LST_ORDITEM_ITEMNAME_2ST_360D
         |      ,LST_ORDITEM_ITEMNAME_3ST_360D
         |      ,MEMBER_FLG
         |      ,MEMBER_LEVEL_DEFINE_ID
         |      ,LEVEL_NAME
         |      ,CAST(COALESCE(EXCHANGE_POINTS,0) AS INT) AS EXCHANGE_POINTS
         |      ,CAST(COALESCE(YH_POINTS_COUNT,0) AS INT) AS YH_POINTS_COUNT
         |      ,CAST(COALESCE(GET_POINTS,0) AS INT) AS GET_POINTS
         |      ,CAST(COALESCE(GET_POINTS_COUNT,0) AS INT) AS GET_POINTS_COUNT
         |      ,CAST(COALESCE(EXPIRED_POINTS,0) AS INT) AS EXPIRED_POINTS
         |      ,CAST(COALESCE(CLEAR_POINTS,0) AS INT) AS CLEAR_POINTS
         |      ,CAST(COALESCE(ACTIVITIES_POINT_COUNT,0) AS INT) AS ACTIVITIES_POINT_COUNT
         |      ,IS_ENTER_WECHAT
         |      ,MEMBER_ORG_ID
         |      ,MEMBER_ORG_NAME
         |      ,STORE_ORG_ID
         |      ,STORE_ORG_NAME
         |      ,IS_MP_ACITIVE_USER
         |      ,cast(COALESCE(VALID_COUNPON_CNT,0) as INT) AS VALID_COUNPON_CNT
         |  FROM global_temp.tmp_dws_im_user_wide_tbl
       """.stripMargin
    println(dws_im_user_wide_tbl_str)
    sql(dws_im_user_wide_tbl_str)
    spark.stop()
    spark.close()
  }
}
