package com.folder.batch.pipeline.transforms;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.folder.utility.LiteralConstant;

public class CommonTimestamp extends DoFn<String, String> {
	
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LoggerFactory.getLogger(CommonTimestamp.class);
		
	@ProcessElement
	public void processElement(ProcessContext c){
			
		SimpleDateFormat sdf = new SimpleDateFormat(LiteralConstant.YYYY_MM_DD_HH_MM_SS.toString());
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		c.output(sdf.format(timestamp));
	}
		
}