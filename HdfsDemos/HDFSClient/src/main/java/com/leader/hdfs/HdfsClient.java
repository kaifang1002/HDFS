package com.leader.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {

        URI uri = new URI("hdfs://hadoop102:8020");

        Configuration configuration = new Configuration();

        // 设置副本数  configuration.set("dfs.replication","1");

        String user = "leader";

        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {

        fs.close();

    }

    // 创建目录
    @Test
    public void testmkdir() throws IOException {

        fs.mkdirs(new Path("/input"));

    }

    /*
     * 参数优先级
     * (低)hdfs-default.xml < hdfs-site.xml < 在项目资源目录下的配置文件 < 代码里的配置（高）
     */

    // 上传
    @Test
    public void testPut() throws IOException {
        // 1.是否删除源文件 2.是否覆盖 3.window文件地址 4.上传地址
        fs.copyFromLocalFile(false, false, new Path("D://role.txt"), new Path("hdfs://hadoop102/input"));
    }

    // 下载
    @Test
    public void testGet() throws IOException {
        // 1.是否删除源文件 2.下载文件地址 3.Windows地址 4.是否文件校验（crc文件校验）
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"), new Path("D://Downloads"), false);
    }

    // 删除
    @Test
    public void testRm() throws IOException {
        fs.delete(new Path("/tmp"), true);
    }

    // 文件的更名和移动
    @Test
    public void testmv() throws IOException {
        fs.rename(new Path("/xiyou/huaguoshan"), new Path("/xiyou/shuiliandong"));
    }

    // 获取文件详细信息
    @Test
    public void fileDetail() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        // 遍历
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println((Arrays.toString(blockLocations)));
        }
    }

    // 判断文件夹和文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件:" + status.getPath().getName());
            } else System.out.println("文件夹:" + status.getPath().getName());

        }
    }
}
/* HDFS的写数据流程
 * 1.客户端向namenode请求上传文件
 * 2.namenode（检查权限；检查目录结构（是否有文件））响应可以上传文件
 * 3.客户端请求上传第一个Block（0-128MB），请返回datenode
 * 4.namenode返回dn1，dn2，dn3节点，表示采用这三个节点存储数据
 * 5.①客户端请求建立Block传输通道dn1；②dn1请求建立通道dn2；③dn2请求建立通道dn3
 * 6.①dn3应答成功；②dn2应答成功；③dn1应答成功 ※ack队列 边存边请求
 * 7.传输数据Packet（64k） Packet（chunk512byte+chunksum4byte）
 */

/* 节点距离
 * d/r/n 最近祖先距离之和
 */

/* 副本选择
 * 第一个副本在Client所处的节点上。如果客户端在集群外，随机选择一个（节点距离最近 上传速度快）
 * 第二个副本在另一个机架的随机一个节点（保证可靠性）
 * 第三个副本在第二个副本所在机架的随机节点（兼顾效率）
 */

/* HDFS的读数据流程
 * 1.客户端向namenode请求下载文件
 * 2.namenode返回目标文件的元数据
 * 3.客户端请求读数据blk_1（读节点最近的）
 * 4.dn1传输数据给客户端
 * 5.一个节点访问量过大则访问其他节点
 * 6.串行读存储（0-128MB）
 */

/* nn和2nn工作机制
 * 内存128G（每个Block占元数据150byte）
 * 1.edits_inprogress_001和fsimage加载编辑日志和镜像文件到内存
 * 2.元数据的增删改请求
 * 3.内存记录操作日志、更新滚动日志 edits_inprogress_001滚动正在写的edits→edits_001、edits_inprogress_002
 * 4.fsimage拷贝到2nn
 * 5.2nn的fsimage和edits_001加载到内存并合并
 * 6.2nn内存生成新的fsimage→fsimage.chkpoint
 * 7.fsimage.chkpoint拷贝到nn
 * 8.fsimage.chkpoint重命名为fsimage
 */

/* fsimage和edits概念
 * fsimage：HDFS文件系统元数据的一个永久性的检查点，包含HDFS文件系统的所有目录和文件inode的序列化信息
 * edits：存放HDFS文件系统的所有更新操作的路径，文件系统客户端执行的所有写操作首先会被记录到edits文件中
 */

/* datenode工作机制
 * 1.datenode启动后向namenode注册（提供块信息）
 * 2.datenode注册成功
 * 3.dn1以后每周期（6小时）上报所有块信息
 * 4.心跳每3秒一次，心跳返回结果带有namenode给该datenode的命令
 * 5.超过10分钟+30秒没有收到datenode2的心跳，则认为该节点不可用
 */

/* 数据完整性
 * crc冗余码校验
 */

/* 掉线实现参数
 * datenode挂掉后 单独开启datenode命令 hdfs --daemon start datenode
 */

/* 面试题
 * 一.常用端口号
 * （hadoop 3.x）
 * HDFS NameNode 内部通用端口号：8020/9000/9820
 * HDFS NameNode 对用户的查询端口：9870
 * Yarn查看任务运行情况的：8088
 * 历史服务器：19888
 * （Hadoop 2.x）
 * HDFS NameNode 内部通用端口号：8020/9000
 * HDFS NameNode 对用户的查询端口：50070
 * Yarn查看任务运行情况的：8088
 * 历史服务器：19888
 * 二.常用的配置文件
 * （hadoop 3.x）
 * core-site.xml  hdfs-site.xml  yarn-site.xml mapred-site.xml workers
 * （hadoop 2.x）
 * core-site.xml  hdfs-site.xml  yarn-site.xml mapred-site.xml slaves
 * HDFS
 * 1.HDFS文件块大小（面试重点）
 * 2.硬盘读写速度 企业中（中小公司）一般128m （大公司）一般256m
 * 3.HDFS的Shell操作（开发重点）
 * 4.HDFS的读写流程（面试重点）
 * 三.Map Reduce
 * 1.InputFormat
 * ①默认的是TextInputFormat kv：key：偏移量   v：一行内容
 * ②处理小文件CombineTextInputFormat 把多个文件合并到一起统一切片
 * 2.Mapper
 * ①setup（）初始化 ②map（）用户的业务逻辑 ③clearup（）关闭资源
 * 3.分区
 * ①默认分区HashPartitioner，默认按照key的hash值%（模与）numreducetask个数
 * ②自定义分区
 * 4.排序
 * ①部分排序：每个输出的文件内部有序
 * ②全排序：一个reduce，对所有数据大排序
 * ③二次排序：自定义排序范畴，实现writableComparable接口，重写compareTo方法 总流量倒叙 按照上行流量 正序
 * 5.Combiner
 * ①前提：不影响最终的业务逻辑（求和可以，求平均值不行）
 * ②提前域聚合 map→解决数据倾斜的一个方法
 * 6.Reducer
 * 用户的业务逻辑
 * ①setup（）初始化
 * ②reduce（）用户的业务逻辑
 * ③clearup（）关闭资源
 * OutputFormat
 * ①默认TextOutputFormat 按行输出到文件
 * ②自定义
 * 四.Yarn
 * 1.Yarn的工作机制（面试题）
 * 2.Yarn的调度器*
 */
//①FIFO/容量/公平
//②apache默认调度器：量调度器；CDH默认调度器：公平调度器
//③容量/公平默认一个default，需要创建多队列
//④中小企业：hive spark flink mr
//⑤中大企业：业务模块：登录/注册/购物车/营销
//⑥好处：解耦 降低风险
//⑦每个调度器特点：相同点：支持多队列，可以借资源，支持多用户；不同点：容量调度器：优先满足先进来的任务执行；公平调度器：在队列里面的任务公平享有对应列资源
//⑧生产环境选择：中小企业：对并发度要求不高，选择容量；中大企业：对并发度要求较高，选择公平
//3.开发需要重点掌握
//①队列运行原理
//②Yarn常用命令
//③核心参数配置
//④配置容量调度器和公平调度器
//⑤tool接口使用