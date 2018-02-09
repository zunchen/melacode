package com.mela.mapred.mapper;

import com.mela.encode.MelaEncoder;
import com.mela.utils.ProvencePhoneSegment;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenzun
 *
 * 原始数据预处理：
 * 1.手机号MD5处理，新加字段
 * 2.IMEI截取前14位，新加字段
 * 3.根据第一列手机号分组
 */
public class ProvencePartMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private Map<String, String> phoneSegmentMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
        phoneSegmentMap = ProvencePhoneSegment.getPhoneSegment();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

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

        List<String> list = new ArrayList<String>();
        list.add(dataArr[0]); // 第一列：源手机号
        list.add(MelaEncoder.getMD5(dataArr[0])); // 第二列：MD5手机号-新增
        list.add(dataArr[1]);
        list.add(dataArr[2]);
        list.add(dataArr[3]);
        list.add(dataArr[4]);
        list.add(dataArr[5]);
        list.add(dataArr[6]);

        // 取手机号前7位-号码段
        String phSeg = StringUtils.substring(dataArr[0], 0, 7);
        String provenceTn = MapUtils.getString(phoneSegmentMap, phSeg);
        if (StringUtils.isBlank(provenceTn)) {
            String[] provenSplit = provenceTn.split(",", -1);
            list.add(provenSplit[0]); // 加省份分类
            list.add(provenSplit[1]); // 加运营商分类
        } else {
            // 没有匹配上省份运营商分类，单独输出
            list.add("\\N");
            list.add("\\N");
        }

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

        // 取手机号前7位-号码段
        String phSeg = StringUtils.substring(data[0], 0, 7);
        String provenceTn = MapUtils.getString(phoneSegmentMap, phSeg);
        if (StringUtils.isBlank(provenceTn)) {
            String[] provenSplit = provenceTn.split(",", -1);
            list.add(provenSplit[0]); // 加省份分类
            list.add(provenSplit[1]); // 加运营商分类
        } else {
            // 没有匹配上省份运营商分类，单独输出
            list.add("\\N");
            list.add("\\N");
        }


        return StringUtils.join(list, "\t");
    }

}
