package com.folder.batch.pipeline.transforms;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import com.folder.utility.LiteralConstant;
import com.google.api.services.bigquery.model.TableRow;



public class FullRefresh extends DoFn<TableRow,TableRow>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(LiteralConstant.YYYY_MM_DD.toString());	
	PCollectionView<String> dateTimeNow;
	
	public FullRefresh(PCollectionView<String> dateTimeNow){
		this.dateTimeNow = dateTimeNow;
		
	}
	
	
	@ProcessElement
	   public void processElement(ProcessContext c) throws ParseException {
		
		TableRow element=c.element();
		String currentTimestamp = c.sideInput(dateTimeNow);
		Date recordDate;
		
		
			recordDate = simpleDateFormat.parse(element.get(LiteralConstant.DW_UPDATE_DTM.toString()).toString());
			
			if (recordDate.before(simpleDateFormat.parse(new java.sql.Timestamp(System.currentTimeMillis()).toString()))
					&& element.get(LiteralConstant.CURRENT_IND.toString()).toString().equals("0")) {
				element.set(LiteralConstant.CURRENT_IND.toString(), "1");
				element.set(LiteralConstant.DW_UPDATE_DTM.toString(), currentTimestamp);
			}
			else if(element.get(LiteralConstant.CURRENT_IND.toString()).toString().equals("0")){
				element.set(LiteralConstant.DW_UPDATE_DTM.toString(), currentTimestamp);
			}
    	
		
		c.output(element);
		
		
	}
	

}
