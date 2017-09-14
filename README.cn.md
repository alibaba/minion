### 介绍

Minion,  实现了一种类似BT，能加强网络传输带宽的途径。它由python语言实现，并基于http的协议设计

Minion 可以像 curl/wget 一样易用. 它同时提供python的api库

### 安装

Minion 命令行工具需要python 2.7, requests库

Minion tracker需要python2.7 django mysql

```
git clone git://github.com/alibaba/minion
cd minion
pip install -r requirements.txt
python setup.py install
```

### 用法

部署自定义tracker服务

```
python tracker/manage.py syncdb
python tracker/manage.py runserver # 线上请使用wsgi/nginx
```

开始使用 minion 命令行工具

```
minion get http://foo.bar/testfile \
     --tracker some.tracker.server \
     --dest-path=/tmp/tops1 \
     --upload-rate 10M \
     --download-rate 100M \
     --callback delete \
     --upload-time 5 \
     --fallback
```

* --tracker 指定tracker server的地址
* --dest-path  下载到的地址，可以指定目录（url后缀为文件名），可为空（当前目录，url后缀为文件）
* --upload-time 指定下载完毕后继续上传的时间长度，默认为60秒
* --download-rate --upload-rate 指定上传下载的速率，可以带单位，如10M，默认10M
* --hash 完成下载后帮忙校验hash值
* --fallback 如果没有peer可用，从源地址下载（源地址下载不限速）
* --callback 目前实现一个选项，delete， 在所有工作结束后会删除文件
* --verbose 为1时打印debug信息，并存进/tmp/minion.log中

### 架构

Minion 工作流如下图

![image](/docs/p2p_flow.png)

PEER:    下载资源的客户机  
TRACKER: 提供P4P资源信息的服务  
SOURCE:  资源的url  


1. PEER0 从 TRACKER 获取上传 SOURCE 的 PEER 列表，返回空
2. PEER0 从 SOURCE 的源地址获取资源
3. PEER0 上传资源到 TRACKER
4. PEER1 从 TRACKER 获取上传 SOURCE 的 PEER 列表，返回包含 PEER0 的列表
5. PEER1 从 PEER0 获取资源
6. PEER1 上传资源到 TRACKER

## License

Minion 适用 GPLv2 开源协议

[English](/README.md)
