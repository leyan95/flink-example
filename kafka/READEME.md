## Windows上启动测试环境

#### 启动zookeepr

1. 解压zk包，创建data和logs文件夹
2. 打开配置文件（D:\Develop\apache-zookeeper-3.6.3-bin\conf\zoo.cfg）配置： dataDir=D:\Develop\apache-zookeeper-3.6.3-bin\data
   dataLogDir=D:\Develop\apache-zookeeper-3.6.3-bin\log
3. 双击执行（D:\Develop\apache-zookeeper-3.6.3-bin\bin\zkServer.cmd）

#### 启动kafka服务

1. 解压kafka包，修改配置群文件（D:\Develop\kafka_2.12-2.8.1\config\zookeeper.properties）配置： dataDir=D:
   \\Develop\\apache-zookeeper-3.6.3-bin\\data
2. 修改配置文件（D:\Develop\kafka_2.12-2.8.1\config\server.properties）配置： log.dirs=D:\\Develop\\kafka_2.12-2.8.1\\kafka-logs
3. 执行命令（.\bin\windows\kafka-server-start.bat .\config\server.properties）

#### 创建主题

1. kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic001

#### 启动生产者

1. 执行命令（kafka-console-producer.bat --broker-list localhost:9092 --topic topic001）

#### 启动消费者

1. kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic topic001 --from-beginning