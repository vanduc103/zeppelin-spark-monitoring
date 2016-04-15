package org.apache.zeppelin.zeppelin_spark_monitoring;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String a = "/a/applications/";
        String[] b = StringUtils.split(a, "/");
        for(int i = 0; i < b.length; i++) {
        	System.out.println(i + "," + b[i]);
        }
        String aa = "2016-04-11T08:30:23.556GMT";
        String fm = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        SimpleDateFormat dfm = new SimpleDateFormat(fm);
        //dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date;
		try {
			date = dfm.parse(aa);
			System.out.println(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Double aaa = 0.344;
		NumberFormat nf = new DecimalFormat("0.#");
		System.out.println(nf.format(aaa));
        System.out.println(Boolean.FALSE);
    }
}
