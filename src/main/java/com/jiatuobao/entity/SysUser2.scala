package com.jiatuobao.entity

/**
  * Author: Felix
  * Desc: 用户样例类
  */
case class SysUser2(
                     id:String,
                     username:String,
                     password:String,
                     realName:String,
                     avatar:String,
                     phone:String,
                     email:String,
                     birthday:String,
                     gender:String,//性别(0神秘人，1男，2女)
                     status:String,//用户状态(1:冻结,0:非冻结)
                     createdTime:String,
                     updatedTime:String,
                     deletedTime:String,
                     registerFrom:String,
                     lastLoginTime:String,
                     changePass:String,//登录之后是否改过密码，0：未改过，1：改过
                     sign:String,
                     registerIp:String,
                     registerUa:String,
                     registerRs:String,
                     lastLoginIp:String,
                     var statusName:String,
                     var changePassName:String,
                     var genderName:String)

