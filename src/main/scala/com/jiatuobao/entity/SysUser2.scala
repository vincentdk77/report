package com.jiatuobao.entity

import scala.beans.BeanProperty

/**
  * Desc: 用户样例类
  */
case class SysUser2(
                    @BeanProperty  id:Integer,
                    @BeanProperty  username:String,
                    @BeanProperty  password:String,
                    @BeanProperty  realName:String,
                    @BeanProperty  avatar:String,
                    @BeanProperty  phone:String,
                    @BeanProperty  email:String,
                    @BeanProperty  birthday:String,
                    @BeanProperty  gender:Integer,//性别(0神秘人，1男，2女)
                    @BeanProperty  status:Integer,//用户状态(1:冻结,0:非冻结)
                    @BeanProperty  createdTime:String,
                    @BeanProperty  updatedTime:String,
                    @BeanProperty  deletedTime:String,
                    @BeanProperty  registerFrom:String,
                    @BeanProperty  lastLoginTime:String,
                    @BeanProperty  changePass:Integer,//登录之后是否改过密码，0：未改过，1：改过
                    @BeanProperty  sign:String,
                    @BeanProperty  registerIp:String,
                    @BeanProperty  registerUa:String,
                    @BeanProperty  registerRs:String,
                    @BeanProperty  lastLoginIp:String,
                    @BeanProperty  var statusName:String,
                    @BeanProperty  var changePassName:String,
                    @BeanProperty  var genderName:String)

