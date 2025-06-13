package com.globalpayments.batch.pipeline.transforms;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TempTransFT1Validation extends DoFn<TableRow, TableRow> implements CommonUtil 
{
	private static final long serialVersionUID = 6470324932430372977L;
	public static final Logger logger = LoggerFactory.getLogger(TempTransFT1Validation.class);

	@ProcessElement
	public void processElement(ProcessContext processContext) {
		TableRow outputTableRow = new TableRow();
		Map<String, String> targetSchema = Utility.getTransFT1Fields();
		Map<String, String> transSchema = Utility.getTrustedTransSchema();
		Map<String, String> skFieldMap = skFieldMapping();

		for (Entry<String, String> fields : targetSchema.entrySet())
  {
			switch (fields.getKey()) {

			case SOURCE_SK:
				outputTableRow.set(fields.getKey(), null != processContext.element().get(fields.getValue())
						? processContext.element().get(fields.getValue()) : DMX_LOOKUP_FAILURE);
				break;

			case CARD_SCHEME_SK:	
			case CARD_TYPE_SK:
			case CHARGE_TYPE_SK:
			case MCC_SK:
			case TRANSACTION_CODE_SK:
			case CORPORATE_SK :
			case REGION_SK :
			case PRINCIPAL_SK :
			case ASSOCIATE_SK :
			case CHAIN_SK :
				if (null == processContext.element().get(skFieldMap.get(fields.getKey()))
						|| processContext.element().get(skFieldMap.get(fields.getKey())).toString().isEmpty())
				{
					outputTableRow.set(fields.getKey(), DMX_LOOKUP_NULL_BLANK);
				} 
				else
				{
					outputTableRow.set(fields.getKey(),
							null == processContext.element().get(fields.getValue())
									|| processContext.element().get(fields.getValue()).toString().isEmpty()
											? DMX_LOOKUP_FAILURE : processContext.element().get(fields.getValue()));
				}
				break;

			case MERCHANT_INFORMATION_SK:
				if ((null == processContext.element().get(MERCHANT_NUMBER)&& null == processContext.element().get(HIERARCHY))
					&& (BLANK.equalsIgnoreCase(processContext.element().get(MERCHANT_NUMBER).toString())
					&& BLANK.equalsIgnoreCase(processContext.element().get(HIERARCHY).toString()))) 
				{
					outputTableRow.set(fields.getKey(), DMX_LOOKUP_NULL_BLANK);
				}
				else
				{
					outputTableRow.set(fields.getKey(), null != processContext.element().get(fields.getValue())
							? processContext.element().get(fields.getValue()) : DMX_LOOKUP_FAILURE);
				}
				break;

			case RESPONSE_DOWNGRADE_CODE:	
			case BATCH_CONTROL_NUMBER:	
				outputTableRow.set(fields.getKey(), processContext.element().get(fields.getKey()));
				break;
				
			default:
				if(transSchema.containsKey(fields.getKey()))
				{
					outputTableRow.set(fields.getKey(), processContext.element().get(fields.getKey()));
				}
				else{
					outputTableRow.set(fields.getKey(), processContext.element().get(fields.getValue()));
				}
			}
  	}
			processContext.output(outputTableRow);
	}
	
	public Map<String,String> skFieldMapping(){
		
		Map<String,String> skFields = new HashMap<>();
		
		skFields.put(CARD_SCHEME_SK, CARD_TYPE);
		skFields.put(CARD_TYPE_SK, CARD_TYPE);
		skFields.put(CHARGE_TYPE_SK, CHARGE_TYPE);
		skFields.put(MCC_SK, MCC);
		skFields.put(TRANSACTION_CODE_SK, TRANSACTION_CODE);
		skFields.put(CORPORATE_SK, CORPORATE);
		skFields.put(REGION_SK, REGION);
		skFields.put(PRINCIPAL_SK, PRINCIPAL);
		skFields.put(ASSOCIATE_SK, ASSOCIATE);
		skFields.put(CHAIN_SK, CHAIN);
		
		return skFields;
	}
}
