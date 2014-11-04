配置：
一、agent.conf
1、snmp：
样例：
	{
	  "snmp": {
	    "ZH4FY9ckRp2xCJIWn919sA": {
	      "category": "snmp_udp",
	      "host": "0.0.0.0",
	      "port": 162,
	      "encoding": "utf-8",
	      "version": "v3",
	      "user": "taoist",
	      "auth_protocol": "md5",
	      "auth_key": "12345678",
	      "priv_protocol": "aes128",
	      "priv_key": "12345678"
	    }
	  }
	}
说明：
	日志类型："snmp"
	asset_id：资产ID，即样例中的"ZH4FY9ckRp2xCJIWn919sA"。必须
	category:使用什么处理器处理，snmp_udp。必须，且只能为snmp_udp
	host：绑定的本机地址。可选，默认值是"0.0.0.0"
	port：绑定的本机端口。可选，默认值162
	encoding：接收到的日志编码格式。可选，默认值"utf-8"
	version：支持的SNMP协议的版本，可选值v3（支持v1、v2c、v3）或为空（只支持v1、v2c）。可选，默认值为空
	－－－－－－－－（version值为v3时以下配置属性有效）
	user：用户名，version值为v3时必填
	auth_protocol：用户认证，可选值md5、sha、空。可选，默认值为空，表示禁用认证
	auth_key：用户密码
	priv_protocol：加密算法，可选值des、aes128、aes192、aes256。可选，默认值为空，表示禁用加密
	priv_key：加密密码

2、Syslog
样例：
	{
	  "syslog": {
	    "Ue7iXsuMT0G6qna5WDxgCQ": {
	      "category": "syslog_tcp",
	      "host": "0.0.0.0",
	      "port": 5014,
	      "encoding": "utf-8"
	    },
	    "ZH4FY9ckRp2xCJIWn919sA": {
	      "category": "syslog_udp",
	      "host": "0.0.0.0",
	      "port": 5014,
	      "encoding": "utf-8"
	    }
	  }
	}
说明：
	日志类型："syslog"
	asset_id：资产ID，即样例中的"Ue7iXsuMT0G6qna5WDxgCQ"和"ZH4FY9ckRp2xCJIWn919sA"。必须
	category:使用什么处理器处理，syslog_tcp或syslog_udp。必须
	host：绑定的本机地址。可选，默认值是"0.0.0.0"
	port：绑定的本机端口。可选，默认值514
	encoding：接收到的日志编码格式。可选，默认值"utf-8"
2、file
样例：
	{
	  "file": {
	    "Ue7iXsuMT0G6qna5WDxgCQ": {
	      "category": "iis",
                "path": "F:/workspace/logger/data/iis",
                "start_position": "end",
                "encoding": "utf-8"
	    },
	    "ZH4FY9ckRp2xCJIWn919sA": {
	      "category": "apache",
           "path": "F:/workspace/logger/data/apache",
           "start_position": "beginning",
           "encoding": "utf-8"
	    }
	  }
	}
说明：
	日志类型："file"
	asset_id：资产ID，即样例中的"Ue7iXsuMT0G6qna5WDxgCQ"和"ZH4FY9ckRp2xCJIWn919sA"。必须
	category:使用什么处理器处理，apache或iis等。必须
	path：文件的存放目录，可以嵌套文件夹。必须
	start_position：读取文件的开始位置.代理启动的时候从新（beginning）从头读取，还是（end）续读上次的位置
	encoding：接收到的日志编码格式。可选，默认值"utf-8"
注：其中的category:默认映射的是agent-class-mapping.properties 取对应的配置属性key 即可
    如 default=com.hansight.kunlun.collector.agent.dir.LineFileLogReader 我们category="default" 就可以了
2.agent-global.properties agent的全局配置
3.资源表配置

    /**
     * 注：
     * 1> index固定格式：logs_yyyyMMdd, 每天一个，日期从数据源@timestamp读取
     * 2> 数据源表将存入关系型数据库(MySql)
     */
        table Asset
        /**
         * Agent中Kafka用的topic为ID
         */
        /**
         * uuid, 作为kafka的topic，Agent及Forworder的配置
         */
        private String id;
        /**
         * 应用日志类型(eg: iis, apache)，添加特殊的类型：other, 匹配DefaultParser;
         * 命名规范，如趋势科技DeepSecurity的防火墙事件：firewall_ds_trend
         */
        private String category;
        /**
         * 普通日志，安全日志(log_*, event_*)；与category组合ES下某个
         * index的type,如：event_firewall_ds_trend
         */
        private String type;
        /**
         * 如IISParser，解析器的别名；DefaultParser为正则解析
         */
        private String parser;
        /**
         * 当parser为正则解析器时，pattern为正则表达式 eg. %{APACHE} \d{4} ；
         * 当parser为分隔符解析器时，pattern为固定格式 如下 {separate:',',fields:['field1','field2','field3',...]}；
         * 其它时，暂定为空
         */
        private String pattern;
        /**
         * Agent所在的host
         */
        private String agentHost;
        /**
         * 数据源的host
         */
        private String host;
        /**
         * 数据源的端口
         */
        private String port;
        /**
         * snmp-trap, snmp, syslog-tcp, syslog-udp, file,ftp,nfs协议
         */
        private String protocol;
        /**
         * 路径(/var/log/message, ftp://192.168.1.2:21/var/log/message)
         */
        private String uri;
        /**
         * utf-8/GBK
         */
        private String encoding;


脚本：
kunlun.sh使用方法：
./kunlun.sh action component
action: start, stop
component: agent, forwarder 

启动agent:
./kunlun.sh start agent
或
./start-agent.sh
停止agent:
./kunlun.sh stop agent
或
./stop-agent.sh

启动forwarder:
./kunlun.sh start forwarder
或
./start-forwarder.sh
停止forwarder:
./kunlun.sh stop forwarder
或
./stop-forwarder.sh