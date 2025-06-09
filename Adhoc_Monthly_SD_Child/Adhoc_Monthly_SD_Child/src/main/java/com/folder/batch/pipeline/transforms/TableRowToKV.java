package com.folder.batch.pipeline.transforms;
import java.text.ParseException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import com.folder.utility.LiteralConstant;
import com.google.api.services.bigquery.model.TableRow;

public class TableRowToKV extends DoFn<TableRow,KV<String,TableRow>>{

	String type;
	PCollectionView<String[]> sdKey;
	
	public TableRowToKV(String type, PCollectionView<String[]> sdKey){
		this.type= type;
		this.sdKey = sdKey;
	}
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c){
				
		StringBuilder keyValue = new StringBuilder("");
		
		String[] softDeleteKey = c.sideInput(sdKey); 
	
		for(String key:softDeleteKey){
			if(key.contains(LiteralConstant.MERCHANT.toString())){
				keyValue.append(Long.parseLong(c.element().get(key.trim()).toString()));
			
			}else {
				keyValue.append(c.element().get(key.trim()));
			}
		}
						
		if(LiteralConstant.MASTER.toString().equals(type)) {
			if(c.element().get(LiteralConstant.CURRENT_IND.toString()).toString().equals("1")){
				c.output(KV.of(keyValue.toString(), c.element()));
			}
		}else{
			
			
			c.output(KV.of(keyValue.toString(), c.element()));
		}
	}
}
