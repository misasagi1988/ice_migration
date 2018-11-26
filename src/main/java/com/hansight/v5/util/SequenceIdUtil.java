package com.hansight.v5.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ck on 2018/8/23.
 */
@Component
public class SequenceIdUtil {
    private final static Logger log = LoggerFactory.getLogger(SequenceIdUtil.class);

    private final static int MAX = 999999;
    private final static int MIN = 100000;
    private final static String SQID = ".sequence";
    private static RandomAccessFile raFile;
    private static AtomicInteger SEQ;

    @PostConstruct
    private void init(){
        File file = new File(SQID);
        if(!file.exists()){
            try {
                boolean create = file.createNewFile();
                if(!create){
                    throw new Exception("create sequence file failed!");
                }
                RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
                accessFile.seek(0);
                accessFile.writeInt(MIN);
                accessFile.close();
            } catch (Exception e) {
                log.error("system exit, fatal err:{}", e);
                System.exit(-1);
            }
        }

        try {
            raFile = new RandomAccessFile(file, "rw");
            SEQ = new AtomicInteger(raFile.readInt());
        } catch (Exception e) {
            log.error("system exit, fatal err:{}", e);
            System.exit(-1);
        }
    }

    public static String getIncrSeqId(){
        if(SEQ.get() > MAX){
            SEQ.set(0);
        }
        int seq = SEQ.incrementAndGet();
        try {
            raFile.seek(0);
            raFile.writeInt(seq);
        }catch (Exception e){

        }
        return String.format("#%06d", seq);
    }



    @PreDestroy
    private void close(){
        if(raFile != null){
            try {
                raFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
