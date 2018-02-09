package com.mela.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ProvencePhoneSegment {

    public static void main(String[] args) throws IOException {
        Map<String, String> map = getPhoneSegment();
        Set<String> keys = map.keySet();
        for (String key : keys) {
            System.out.println(key + "|" + (String) map.get(key));
        }
    }

    public static Map<String, String> getPhoneSegment() throws IOException {

        Map<String, String> provPhoneSegMap = new HashMap<String, String>();
        File fileDir = new File("/Volumes/NO NAME/MELAWORK/pro_seg/");
        File[] provenceList = fileDir.listFiles();
        for (File provence : provenceList) { // 省份目录
            String provenceName = provence.getName();
            if (StringUtils.startsWith(provenceName, ".")) {
                continue;
            }

            File[] phoneSegFiles = provence.listFiles();
            if (ArrayUtils.isEmpty(phoneSegFiles)) {
                System.out.println(provenceName + "," + provence.getPath());
                continue;
            }
            for (File phoneSegFile : phoneSegFiles) { // 号码段文件
                String serverName = phoneSegFile.getName(); // 运营商名称

                if (StringUtils.startsWith(serverName, ".")) {
                    continue;
                }

                List<String> phoneSegs = FileUtils.readLines(phoneSegFile);
                if (CollectionUtils.isEmpty(phoneSegs)) {
                    continue;
                }
                int sum = 0;
                for (String phoneSeg : phoneSegs) {
                    if (StringUtils.isBlank(phoneSeg)) {
                        System.out.println(provenceName + "," + phoneSegFile.getPath());
                        continue;
                    }
                    provPhoneSegMap.put(StringUtils.trim(phoneSeg), provenceName + "," + serverName.substring(0, serverName.indexOf('.')));
                    sum++;
                }
//                System.out.println(provenceName + "," + serverName + "," + sum);
            }
        }

        System.out.println(provPhoneSegMap.size());

//        FileUtils.readLines(new File());

        return provPhoneSegMap;
    }

}
