package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.NavigableMap;

public class HbaseAPIs {
    // 声明静态配置
    static Configuration conf = null;
    static {
        //获得Configuration实例并进行相关设置
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.150.137,192.168.150.136");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //conf.set("zookeeper.znode.parent","/hbase");
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String[] familyNames) throws IOException {

        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);

        //获得Admin接口
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"表已存在");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        admin.createTable(tableDescriptor);
        System.out.println(tableName + "创建成功");
        admin.close();
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);

        //获得Admin接口
        Admin admin = connection.getAdmin();

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println(tableName + "删除成功");

    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);

        //获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 为表添加数据（适合知道有多少列族的固定表）
     * 
     * @param rowKey rowKey
     * @param tableName 表名
     * @param column1 第一个列族列表
     * @param value1 第一个列的值的列表
     * @param column2 第二个列族列表
     * @param value2 第二个列的值的列表
     */
    public static void addData(String rowKey, String tableName,
                               String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);

        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey

        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表

        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("article")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) { // author列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("数据添加成功!");
    }

    /**
     * 根据rwokey,family,col查询
     * @param rowKeys rowKey
     * @param tableName 表名
     */
    public static void getResult(String tableName, String rowKeys, String family,String column,String values,Integer version)
            throws IOException {
        //多条件查询
        /*FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        fl.addFilter(filter);
        scan.setFilter(fl);*/

        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(rowKeys));
        Filter filter_family = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(family));
        Filter filter_column = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(column));
        Filter filter_value = new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(values));
        Scan scan = new Scan();
        scan.setFilter(filter_value);
        scan.setMaxVersions(version);
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表

        try {
            ResultScanner scans = table.getScanner(scan);

            for (Result rst : scans) {
                // family    qualifiers     values
                NavigableMap<byte[], NavigableMap<byte[],byte[]>> familyMap = rst.getNoVersionMap();

                for(byte[] fByte : familyMap.keySet()){
                    String rowKey = Bytes.toString(rst.getRow());

                    NavigableMap<byte[],byte[]> quaMap = familyMap.get(fByte);

                    String familyName = Bytes.toString(fByte);

                    for(byte[] quaByte : quaMap.keySet()){
                        byte[] valueByte = quaMap.get(quaByte);
                        String quaName = Bytes.toString(quaByte);
                        String value = Bytes.toString(valueByte);
                        String result = String.format("rowKey : %s | family : %s | qualifiers : %s | value : %s",
                                rowKey, familyName, quaName, value);
                        System.out.println(result);
                    }
                }
            }
            scans.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 遍历查询hbase表
     * 
     * @param tableName 表名
     */
    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException {

        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表

        try {
            ResultScanner scans = table.getScanner(scan);

            for (Result rst : scans) {
                String rowKey = Bytes.toString(rst.getRow());
                // family    qualifiers     values
                NavigableMap<byte[], NavigableMap<byte[],byte[]>> familyMap = rst.getNoVersionMap();

                for(byte[] fByte : familyMap.keySet()){

                    NavigableMap<byte[],byte[]> quaMap = familyMap.get(fByte);

                    String familyName = Bytes.toString(fByte);

                    for(byte[] quaByte : quaMap.keySet()){
                        byte[] valueByte = quaMap.get(quaByte);
                        String quaName = Bytes.toString(quaByte);
                        String value = Bytes.toString(valueByte);
                        String result = String.format("rowKey : %s | family : %s | qualifiers : %s | value : %s",
                                rowKey, familyName, quaName, value);
                        System.out.println(result);
                    }
                }
            }
            scans.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 更新表中的某一列
     * @param tableName 表名
     * @param rowKey rowKey
     * @param familyName 列族名
     * @param columnName 列名
     * @param value 更新后的值
     */
    public static void updateTable(String tableName, String rowKey,
                                   String familyName, String columnName, String value)
            throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("更新表成功!");
    }

    /**
     * 删除指定的列
     * 
     * @param tableName 表名
     * @param rowKey rowKey
     * @param familyName 列族名
     * @param columnName 列名
     */
    public static void deleteColumn(String tableName, String rowKey,
                                    String familyName, String columnName) throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        //deleteColumn.addFamily(Bytes.toBytes(familyName));
        deleteColumn.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        table.close();
        System.out.println(familyName + ":" + columnName + "删除成功!");
    }

    /**
     * 删除指定的列
     * 
     * @param tableName 表名
     * @param rowKey rowKey
     */
    public static void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteColumn);
        table.close();
        System.out.println(rowKey + ":" + "删除成功!");
    }


    public static void scan(String tableName) throws IOException {

        //获得Connection实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result rst : rs) {
            for(Cell cell : rst.rawCells()){
                System.out.print("行为："+new String(CellUtil.cloneRow(cell)));
                System.out.print("列簇为："+new String(CellUtil.cloneFamily(cell)));
                System.out.print("列修饰符为："+new String(CellUtil.cloneQualifier(cell)));
                System.out.println("值为："+new String(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // 创建表
        /*String tableName = "hitachi";
        String[] family = { "article", "author" };
        createTable(tableName, family);*/

        // 为表添加数据

        /*String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        addData("rowkey1", "wangxin", column1, value1, column2, value2);
        addData("rowkey2", "wangxin", column1, value1, column2, value2);
        addData("rowkey3", "wangxin", column1, value1, column2, value2);*/

        // 遍历查询
        //getResultScann("wangxin", "", "");

        // 查询
        getResult("wangxin", "rowkey1", "article", "name", "HBase is the Hadoop database. Use it when you nee\n" +
                "                    d random, realtime read/write access to your Big Dat\n" +
                "                    a.",1);

        // 更新列
        //updateTable("wangxin", "rowkey1", "author", "name", "bin");

        // 删除一列
        //deleteColumn("wangxin", "rowkey3", "author", "nickname");

        // 删除所有列
        //deleteAllColumn("wangxin", "rowkey1");

        // 删除表
        //dropTable("wangxin");

        //scan("wangxin");

    }
}