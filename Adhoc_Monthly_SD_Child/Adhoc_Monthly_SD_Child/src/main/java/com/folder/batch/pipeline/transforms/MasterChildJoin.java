package com.folder.batch.pipeline.transforms;
/**
 * This Class is used to set current_ind value to 1 for inactive child records.
 * @author Bitwise Offshore
 * 
 */
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.folder.utility.LiteralConstant;
import com.google.api.services.bigquery.model.TableRow;

public class MasterChildJoin extends DoFn<KV<String, CoGbkResult>, TableRow>
{
	
	private static final long serialVersionUID = 1295682609113443582L;
	
	TupleTag<TableRow> kvMasterTuple;
	TupleTag<TableRow> kvChildTuple;
	PCollectionView<String> dateTimeNow;
	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(LiteralConstant.YYYY_MM_DD.toString());
	public MasterChildJoin(TupleTag<TableRow> kvMasterTuple, TupleTag<TableRow> kvChildTuple, PCollectionView<String> dateTimeNow) {
		this.kvMasterTuple = kvMasterTuple;
		this.kvChildTuple = kvChildTuple;
		this.dateTimeNow = dateTimeNow;
	}

	@ProcessElement
   public void processElement(ProcessContext c) throws ParseException {
		KV<String, CoGbkResult> e = c.element();
		
		Date recordDate;

	    Iterable<TableRow> childElements = e.getValue().getAll(kvChildTuple);
	    
	    Iterable<TableRow> masterElements = e.getValue().getAll(kvMasterTuple);
	   
	    String currentTimestamp = c.sideInput(dateTimeNow);
	    
	    Iterator<TableRow> masterIter = masterElements.iterator();
	    Iterator<TableRow> childIter = childElements.iterator();
	    
	    while(childIter.hasNext()){
	    	TableRow element = childIter.next().clone();
	    	
	    	if (masterIter.hasNext()) {
	    		element.set(LiteralConstant.CURRENT_IND.toString(), "1");
	    		element.set(LiteralConstant.DW_UPDATE_DTM.toString(), currentTimestamp);
	    	}
	    	
	    	else{
				recordDate = simpleDateFormat.parse(element.get(LiteralConstant.DW_UPDATE_DTM.toString()).toString());
				
				if (recordDate.before(simpleDateFormat.parse(new java.sql.Timestamp(System.currentTimeMillis()).toString()))
						&& element.get(LiteralConstant.CURRENT_IND.toString()).toString().equals("0")) {
					element.set(LiteralConstant.CURRENT_IND.toString(), "1");
					element.set(LiteralConstant.DW_UPDATE_DTM.toString(), currentTimestamp);
				}
				else if(element.get(LiteralConstant.CURRENT_IND.toString()).toString().equals("0")){
					element.set(LiteralConstant.DW_UPDATE_DTM.toString(), currentTimestamp);
				}
	    	}
	    	c.output(element);
	    
	    }
	    
	}
}