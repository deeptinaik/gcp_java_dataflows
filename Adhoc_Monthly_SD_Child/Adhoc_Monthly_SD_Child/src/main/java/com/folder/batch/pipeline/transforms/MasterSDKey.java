package com.folder.batch.pipeline.transforms;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.folder.utility.LiteralConstant;

public class MasterSDKey extends DoFn<String, String[]> {
	
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LoggerFactory.getLogger(MasterSDKey.class);
	ValueProvider<String> valueProvider;
	public MasterSDKey(ValueProvider<String> valueProvider){
		this.valueProvider = valueProvider;
	}
	
	@ProcessElement
	public void processElement(ProcessContext c){
		String[] sdKeyArray = valueProvider.get().split(LiteralConstant.SOFT_DELETE_KEY_DELIMITER.toString());
		if(null != sdKeyArray && sdKeyArray.length == 2){
			
			String childKey = sdKeyArray[0].trim();
			logger.info("MasterSDKey :{}",childKey);			
			c.output(childKey.split(LiteralConstant.SOFT_DELETE_KEY_DELIMITER1.toString()));
		}
		
	}
}