package com.guanshi.cloudhotel.entity

import java.sql.Timestamp
import java.util.Date

object Entitys {

  case class LiveDays(hotelId: Int, roomId: Int, month: String, roomMoney: java.math.BigDecimal, liveDays: Int, monthDays: Int)

  case class BookedRoom(bookedroom_id: Int, livetype: String, room_money: java.math.BigDecimal, booktime: Timestamp, bookvalid_flag: Int, paymoney: java.math.BigDecimal, disable_reason: String, booking_id: Int, roominfo_id: Int, disable_staff_id: Int, disable_time: Timestamp, checkin_room_id: Int)

  case class CommentInfo(platForm: String, hotelId: Int, comment: String, sumScore: Int, cleanScore: Int, serviceScore: Int, sercurityScore: Int, styleScore: Int)

  case class CommentAnalyze(count: Int, maxScore: Int, minScore: Int, cleanAvg: Double, serviceAvg: Double, sercurityAvg: Double, styleAvg: Double)

}
