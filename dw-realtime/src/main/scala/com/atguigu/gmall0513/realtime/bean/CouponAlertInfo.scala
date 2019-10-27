package com.atguigu.gmall0513.realtime.bean

/**
  * mid  	设备id
  * uids	领取优惠券登录过的uid
  * itemIds	优惠券涉及的商品id
  * events  	发生过的行为
  * ts	发生预警的时间戳
  */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)
