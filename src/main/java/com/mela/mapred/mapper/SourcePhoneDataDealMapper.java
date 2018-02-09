package com.mela.mapred.mapper;

import com.mela.encode.MelaEncoder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author chenzun
 *
 * 原始数据预处理：
 * 1.手机号MD5处理，新加字段
 * 2.IMEI截取前14位，新加字段
 * 3.根据第一列手机号分组
 */
public class SourcePhoneDataDealMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            return;
        }

        String[] dataArr = value.toString().split("\t", -1);
        // 原始数据字段长度不够7位，此条数据过滤掉
        if (dataArr.length < 7) {
            return;
        }

        // 格式化数据
        Text phoneData = new Text(transData(dataArr));

        context.write(new Text(dataArr[0]), phoneData);
    }

    private String transData(String[] data) {
        List<String> list = new ArrayList<String>();
        list.add(data[0]); // 第一列：源手机号
        list.add(MelaEncoder.getMD5(data[0])); // 第二列：MD5手机号-新增
        list.add(data[1]);
        list.add(data[2]);
        list.add(data[3]);
        list.add(data[4]);
        list.add(data[5]);
        list.add(data[6]);


        return StringUtils.join(list, "\t");
    }

}
