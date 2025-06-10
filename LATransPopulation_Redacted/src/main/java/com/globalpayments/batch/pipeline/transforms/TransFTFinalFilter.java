package com.globalpayments.batch.pipeline.transforms;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.Generators;
import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TransFTFinalFilter extends DoFn<TableRow, TableRow> implements CommonUtil {
	private static final long serialVersionUID = -8312289848673252195L;
	public static final Logger logger = LoggerFactory.getLogger(TransFTFinalFilter.class);

	@ProcessElement
	public void processElement(ProcessContext processContext) throws ParseException {
		TableRow outputTableRow = new TableRow();
		Map<String, String> finalSchema = Utility.finalFields();

		for (Entry<String, String> fieldName : finalSchema.entrySet()) {
			try {
				switch (fieldName.getKey()) {

				case LA_TRANS_FT_UUID:
					outputTableRow.set(fieldName.getKey(), Generators.timeBasedGenerator().generate().toString());
					break;

				case AVS_RESPONSE_CODE_SK:
					if (null == processContext.element().get(AVS_RESPONSE_CODE)
							|| processContext.element().get(AVS_RESPONSE_CODE).toString().isEmpty()) 
					{
						outputTableRow.set(fieldName.getKey(), DMX_LOOKUP_NULL_BLANK);
					} 
					else if (null == processContext.element().get(fieldName.getValue())
							|| processContext.element().get(fieldName.getValue()).toString().isEmpty()) 
					{
						outputTableRow.set(fieldName.getKey(), DMX_LOOKUP_FAILURE);
					} 
					else 
					{
						outputTableRow.set(fieldName.getKey(), processContext.element().get(fieldName.getValue()));
					}
					break;

				case TRANSACTION_CURRENCY_CODE_SK:
					if (null == processContext.element().get(fieldName.getValue())
							|| processContext.element().get(fieldName.getValue()).toString().isEmpty()) 
					{
						outputTableRow.set(fieldName.getKey(), DMX_LOOKUP_FAILURE);
					} 
					else
					{
						outputTableRow.set(fieldName.getKey(), processContext.element().get(fieldName.getValue()));
					}
					break;

				case FILE_DATE:
				case TRANSACTION_DATE:
				case AUTHORIZATION_DATE:
				case FILE_DATE_ORIGINAL:	
					outputTableRow.set(fieldName.getKey(),
							Utility.parseDate(processContext.element().get(fieldName.getKey()), DATE_FORMAT_YYYYMMDD)); // DATE_FORMAT_MMDDYY

					break;

				case CAR_RENTAL_CHECKOUT_DATE:
				case LODGING_CHECKIN_DATE:
				case DEPOSIT_DATE:
					outputTableRow.set(fieldName.getKey(),
							Utility.parseDate(processContext.element().get(fieldName.getKey()), DATE_FORMAT_YYYYMMDD)); // DATE_FORMAT_YYMMDD
					break;

				case MERCHANT_NUMBER:
					outputTableRow.set(MERCHANT_NUMBER,
							Utility.trimLeadingZeros(null != processContext.element().get(MERCHANT_NUMBER)
									? processContext.element().get(MERCHANT_NUMBER).toString()
									: null));
					break;

				case MERCHANT_NUMBER_INT:
					if (null != processContext.element().get(MERCHANT_NUMBER)) {
						outputTableRow.set(MERCHANT_NUMBER_INT, Long.toString(
								Long.parseLong(processContext.element().get(MERCHANT_NUMBER).toString().trim())));
					}
					break;

				case CARD_NUMBER_SK:
					if (null != processContext.element().get(CARD_NUMBER_SK)) {
						outputTableRow.set(CARD_NUMBER_SK, Long.toString(
								Long.parseLong(processContext.element().get(CARD_NUMBER_SK).toString().trim())));
					}
					break;

				case POS_ENTRY_CODE_DESC:
					if (null != processContext.element().get(fieldName.getValue())) {
						outputTableRow.set(fieldName.getKey(), processContext.element().get(fieldName.getValue()));
					}
					break;

				default:
					outputTableRow.set(fieldName.getKey(), processContext.element().get(fieldName.getKey()));
				}

			} catch (ParseException parseException) {
				logger.error(parseException.toString());
			} catch (Exception genericexception) {
				logger.error(genericexception.toString());
			}
		}
		outputTableRow.set(CREATE_DATE_TIME,
				LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")).toString());
		outputTableRow.set(UPDATE_DATE_TIME,
				LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")).toString());
		processContext.output(outputTableRow);
	}
}
