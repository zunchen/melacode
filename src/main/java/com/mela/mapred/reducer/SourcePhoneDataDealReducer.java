package com.mela.mapred.reducer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author chenzun
 *
 * 手机号数据清洗合并:
 * 1.
 */
public class SourcePhoneDataDealReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isPatchEnd = false;
        Iterator<Text> iterator = values.iterator();

        Set<Integer> emptyIndexSet = new HashSet<Integer>();
        String[] firstData = null;
        int num = 0;
        while (iterator.hasNext()) {
            String[] split = iterator.next().toString().split("\t", -1);

            // 如果是第一条，先填充firstData
            if (num == 0) {
                firstData = split;
                for (int i = 2; i < split.length; i++) { // 从第三列开始，前两列为：手机号，MD5手机号（肯定不为空)
                    // 记录第一行为空的index位置
                    if (StringUtils.isBlank(split[i]) || "\\N".equalsIgnoreCase(StringUtils.trim(split[i]))) {
                        emptyIndexSet.add(i);
                    }
                }
                continue;
            }

            // 如果emptyIndexSet为空，说明第一条没有需要补/合并的字段
            if (CollectionUtils.isEmpty(emptyIndexSet)) {
                break;
            }

            // 如果不是第一条，给第一条补数据
            for (int index : emptyIndexSet) {
                if (StringUtils.isBlank(split[index]) || "\\N".equalsIgnoreCase(StringUtils.trim(split[index]))) {
                    continue;
                }
                // 当前此条数据的index位置字段值非空，给firstData相应字段补值
                firstData[index] = split[index];
                emptyIndexSet.remove(index); // 同时清除emptyIndexSet相应位置
            }


            num++;
        }

        // 将补全之后的数据firstData输出到文件
        context.write(NullWritable.get(), new Text(StringUtils.join(firstData, "\t")));
    }
}
