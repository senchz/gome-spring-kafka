package com.zsq.bean;

import java.util.Date;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
public class Message {
    private Long id;
    private String msg;
    private Date sendTime;
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getMsg() {
        return msg;
    }
    public void setMsg(String msg) {
        this.msg = msg;
    }
    public Date getSendTime() {
        return sendTime;
    }
    public void setSendTime(Date sendTime) {
        this.sendTime = sendTime;
    }
}
