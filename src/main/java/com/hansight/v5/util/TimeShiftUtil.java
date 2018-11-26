package com.hansight.v5.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/** 
* @ClassName: TimeShiftUtil 
* @Description: TODO
* @author xuezhi_xu
* @date 2016年8月1日 下午1:33:48 
*  
*/
public class TimeShiftUtil {
	
	private static final TimeShiftUtil instance = new TimeShiftUtil();
	
	public static TimeShiftUtil getInstance(){
		
		return instance;
	}
	
	Logger logger = LoggerFactory.getLogger(TimeShiftUtil.class);
	
	/**
	 * 提供毫秒获得yyyy-MM-dd HH:mm:ss
	 * @param time
	 * @return
	 */
	public String longToStringGetYMDHMS(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(time);
	}
	
	/**
	 * 提供毫秒获得 yyyy-MM-dd HH:mm:ss S
	 * @param time
	 * @return
	 */
	public String longToStringGetYMDHMSS(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S");
		return sdf.format(time);
	}
	
	/**
	 * 提供毫秒获得 yyyy-MM-dd HH-mm-ss S
	 * @param time
	 * @return
	 */
	public String longToStringGetYMDHMSSs(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss S");
		return sdf.format(time);
	}
	
	/**
	 * 提供毫秒 获得yyyyMMdd
	 * @param time
	 * @return
	 * @data 2013-2-28 下午5:38:01
	 */
	public String longToStringGetYMD(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(time);
	}
	/**
	 * 提供毫秒 获得yyyyMMdd_HHmm
	 * @param time
	 * @return
	 */
	public String longToStringGetYMD_HM(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss_S");
		return sdf.format(time);
	}
	
	/**
	 * 提供毫秒 获得dd HH:mm
	 * @param time
	 * @return
	 */
	public String longToStringGetDHM(long time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd HH:mm");
		String timeStart = sdf.format(new Date(time));
		
		return timeStart;
	}
	
	/**
	 * 提供毫秒 获得dd HH:mm:ss
	 * @param time
	 * @return
	 */
	public String longToStringGetDHMS(long time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd HH:mm:ss");
		String timeStart = sdf.format(new Date(time));
		
		return timeStart;
	}
	
	/**
	 * 提供毫秒 获得yyyy-MM-dd HH:mm
	 * @param time
	 * @return
	 */
	public String longToStringGetYMDHM(long time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		String timeStart = sdf.format(new Date(time));
		
		return timeStart;
	}
	
	/**
	 * 提供 yyyy-MM-dd HH:mm获得毫秒
	 * @param time
	 * @return
	 */
	public long stringToLongByYMDHM(String time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date date;
		try {
			date = sdf.parse(time);
			return date.getTime();
		} catch (ParseException e) {
			logger.error("printStackTrace error: ", e);
		}
		return 0;
	}
	
	/**
	 * 提供 yyyy-MM-dd HH:mm:ss S 获得毫秒
	 * @param time
	 * @return
	 */
	public long stringToLongByYMDHMSS(String time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S");
		Date date;
		try {
			date = sdf.parse(time);
			return date.getTime();
		} catch (ParseException e) {
			logger.error("printStackTrace error: ", e);
		}
		return 0;
	}
	
	/**
	 * 提供 yyyyMMdd 获得毫秒
	 * @param time
	 * @return
	 */
	public long stringToLongByYMD(String time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date;
		try {
			date = sdf.parse(time);
			return date.getTime();
		} catch (ParseException e) {
			logger.error("printStackTrace error: ", e);
		}
		return 0;
	}
	
	/**
	 * 提供 yyyy-MM-dd HH:mm:ss，获得毫秒
	 * @param time
	 * @return
	 */
	public long millisecondStringLong(String time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try {
			date = sdf.parse(time);
			return date.getTime();
		} catch (ParseException e) {
			logger.error("printStackTrace error: ", e);
		}
		return 0;
	}
	
	
	/**
	 * 获得当前时间
	 * @return
	 */
	public long getTime() {
		return System.currentTimeMillis();
	}
	

}
