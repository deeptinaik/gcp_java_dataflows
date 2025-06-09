package com.folder.batch.pipeline.transforms;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.folder.utility.LiteralConstant;

public class ChildSDKey extends DoFn<String, String[]> {
	
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LoggerFactory.getLogger(ChildSDKey.class);
	ValueProvider<String> valueProvider;
	public ChildSDKey(ValueProvider<String> valueProvider){
		this.valueProvider = valueProvider;
	}
	
	@ProcessElement
	public void processElement(ProcessContext c){
		String[] sdKeyArray = valueProvider.get().split(LiteralConstant.SOFT_DELETE_KEY_DELIMITER.toString());
		if(null != sdKeyArray && sdKeyArray.length == 2){
			
			String childKey = sdKeyArray[1].trim();
			logger.info("childKey :{}",childKey);			
			c.output(childKey.split(LiteralConstant.SOFT_DELETE_KEY_DELIMITER1.toString()));
		}
		
	}
}