# 目录文件说明


### ONS模式进行鉴权的时候需要用到的对应包

	* commons-codec-1.9.jar
	* ons-sasl-client-0.1.jar

	* 将其放到
	* /usr/share/logstash/vendor/bundle/jruby/1.9/gems/logstash-input-kafka-5.1.11/vendor/jar-dependencies/runtime-jars/
	* 下面

### 阿里云账号配置文件
	
	* jaas.conf

	* AccessKey	请修改为阿里云账号的AccessKey
	* SecretKey	请修改为阿里云账号的SecretKey

### 证书
	
	* kafka.client.truststore.jks

### logstash配置文件

	* logstash_input.conf		
        * bootstrap_servers	        请根据region列表进行选取
	* topics		        请修改为MQ控制台上申请的Topic
	* group_id		        请修改为MQ控制台申请的CID
	* security_protocol	        SASL_SSL，无需修改
	* sasl_mechanism		ONS，无需修改
	* jaas_path		        请修改成agent/jaas.conf的存放位置
	* ssl_truststore_location	请修改成agent/kafka.client.truststore.jks存放的位置
        * ssl_truststore_password	KafkaOnsClient，无需修改
