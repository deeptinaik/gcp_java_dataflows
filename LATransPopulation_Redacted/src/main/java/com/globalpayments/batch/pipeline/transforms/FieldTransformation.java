
package com.globalpayments.batch.pipeline.transforms;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.TableFieldFilter;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class FieldTransformation extends DoFn<TableRow, TableRow> implements CommonUtil
{
	private static final long serialVersionUID = -3995666265798761858L;
	public static final Logger logger = LoggerFactory.getLogger(TableFieldFilter.class);
	private DecimalFormat decimalFormat;
	public static final int exponent = 5;

	
	public FieldTransformation()
	{
	decimalFormat = new DecimalFormat();
	decimalFormat.setGroupingUsed(false);
	decimalFormat.setMaximumFractionDigits(9);
	decimalFormat.setMaximumIntegerDigits(29);

	}
	
	@ProcessElement
	public void processElement(ProcessContext processContext)
	{
		TableRow outputTableRow = new TableRow();
		TableRow record=processContext.element();
		
		Map<String, String> trustedSchema = Utility.getTrustedTransSchema();
		

		for (Entry<String, String> fieldName : trustedSchema.entrySet()) {
			try {
			
				switch (fieldName.getKey()) {

				case FILE_DATE_ORIGINAL:
				case TRANSACTION_DATE:
					String inputDate = (null != record.get(fieldName.getValue())? record.get(fieldName.getValue()).toString().trim() : BLANK);

					if (!inputDate.isEmpty() && 6 == inputDate.length()) {
						StringBuilder inputDateBuilder = new StringBuilder("20").append(inputDate.substring(4))
								.append(inputDate.substring(0, 2)).append(inputDate.substring(2, 4));

						outputTableRow.set(fieldName.getKey(),Utility.isValidDate(inputDateBuilder.toString()) ? inputDateBuilder.toString() : BLANK);
					}
					break;


				case TRANSACTION_AMOUNT:
					if (null != record.get(TRANSACTION_AMOUNT_NEW))
					{
						BigDecimal transactionAmount = new BigDecimal(
								Long.parseLong(record.get(TRANSACTION_AMOUNT_NEW).toString()))
										.scaleByPowerOfTen(-exponent);
						outputTableRow.set(TRANSACTION_AMOUNT, decimalFormat.format(transactionAmount));
					}
					break;

				case SETTLED_AMOUNT:
					if (null != record.get(SETTLED_AMOUNT)) {
						BigDecimal settledAmount = new BigDecimal(Long.parseLong(record.get(SETTLED_AMOUNT).toString()))
								.scaleByPowerOfTen(-exponent);
						outputTableRow.set(SETTLED_AMOUNT, decimalFormat.format(settledAmount));
					}
					break;

				case AUTHORIZATION_AMOUNT:
				case SUPPLEMENTAL_AUTHORIZATION_AMOUNT:
					if (null != record.get(fieldName.getValue())
							&& !BLANK.equalsIgnoreCase(record.get(fieldName.getValue()).toString().trim()))
					
					{
						StringBuilder amountBuilder = new StringBuilder(
								record.get(fieldName.getValue()).toString().substring(0, 7)).append(DOT)
										.append(record.get(fieldName.getValue()).toString().substring(7));
						outputTableRow.set(fieldName.getKey(), amountBuilder.toString());
					} else {
						outputTableRow.set(fieldName.getKey(), ZERO);
					}
					break;

				case LODGING_CHECKIN_DATE:
				case CAR_RENTAL_CHECKOUT_DATE:
					String tempDate = (null != record.get(fieldName.getValue())
							? record.get(fieldName.getValue()).toString().trim()
							: BLANK);

					if (!tempDate.isEmpty() && 6 == tempDate.length()) {
						StringBuilder inputDateBuilder = new StringBuilder("20").append(tempDate);

						outputTableRow.set(fieldName.getKey(),
								Utility.isValidDate(inputDateBuilder.toString()) ? inputDateBuilder.toString() : BLANK);
					}
					break;

				case FILE_DATE:					
				case DEPOSIT_DATE:
					if (null != record.get(fieldName.getValue())
							&& !record.get(fieldName.getValue()).toString().isEmpty()
							&& 6 == record.get(fieldName.getValue()).toString().length()) {
						StringBuilder inputDateBuilder = new StringBuilder("20")
								.append(record.get(fieldName.getValue()));

						outputTableRow.set(fieldName.getKey(),
								Utility.isValidDate(inputDateBuilder.toString()) ? inputDateBuilder.toString() : BLANK);
					}
					break;

				case AUTHORIZATION_DATE:
					if (null != record.get(TRANSACTION_DATE) && !record.get(TRANSACTION_DATE).toString().isEmpty()
							&& null != record.get(fieldName.getKey())
							&& !record.get(fieldName.getKey()).toString().isEmpty()) {

						StringBuilder authDate = new StringBuilder();
						if ("01".equalsIgnoreCase(record.get(TRANSACTION_DATE).toString().substring(0, 2))
								&& "12".equalsIgnoreCase(record.get(fieldName.getKey()).toString().substring(0, 2))) {

							authDate.append("20")
									.append(Integer.toString(
											Integer.parseInt(record.get(TRANSACTION_DATE).toString().substring(4)) - 1))
									.append(record.get(fieldName.getKey()).toString());
						} else {

							authDate.append("20").append(record.get(TRANSACTION_DATE).toString().substring(4))
									.append(record.get(fieldName.getKey()).toString());
						}

						outputTableRow.set(fieldName.getKey(), authDate.toString());
					}
					break;

				case BATCH_CONTROL_NUMBER:
					if (null != record.get(DEPOSIT_DATE) && null != record.get(CASH_LETTER_NUMBER))
						outputTableRow.set(BATCH_CONTROL_NUMBER, createBatchCntrlNum(
								record.get(DEPOSIT_DATE).toString(), record.get(CASH_LETTER_NUMBER).toString()));

					break;

				case TRANSACTION_TIME:
				case FILE_TIME:
					String timeField = Utility.getValidTime(record.get(fieldName.getValue()));
					if (null != timeField)
						outputTableRow.set(fieldName.getKey(), timeField);

					break;

				case TRANSACTION_IDENTIFIER:
					String transactionId = null != record.get(TRANSACTION_ID)
							? record.get(TRANSACTION_ID).toString()
							: BLANK;

					outputTableRow.set(TRANSACTION_IDENTIFIER, transactionId.replaceAll(Pattern.quote(" "), BLANK));
					break;

				case MERCHANT_NUMBER_INT:
					outputTableRow.set(MERCHANT_NUMBER_INT, Utility.trimLeadingZeros(
							null != record.get(MERCHANT_NUMBER) ? record.get(MERCHANT_NUMBER).toString() : null));
					break;

				case HIERARCHY:
					StringBuilder hierarchyBuilder = new StringBuilder();
					for (String hierarchyField : fieldName.getValue().split(",")) {
						hierarchyBuilder.append(
								null != record.get(hierarchyField) ? record.get(hierarchyField).toString() : BLANK);
					}
					outputTableRow.set(HIERARCHY, hierarchyBuilder.toString());
					break;

				default:
					outputTableRow.set(fieldName.getKey(), record.get(fieldName.getValue()));
				}
			} 
			catch (ParseException parseException)
			{

			} catch (Exception genericexception) 
			{

			}
		}
		processContext.output(outputTableRow);
	}

	private String createBatchCntrlNum(String depositeDate, String cashLetterNumber) throws ParseException {

		String cashLetterSubString = "";
		try {
			cashLetterSubString = cashLetterNumber.substring(3, 9);
		} catch (IndexOutOfBoundsException indexOutOfBoundsException) {
		}

		DateFormat format = new SimpleDateFormat(DATE_FORMAT_YYYYMMDD);
		format.setLenient(false);
		Date date = format.parse(new StringBuilder("20").append(depositeDate).toString());
		String yearDaysCount = String.valueOf(new DateTime(date.getTime()).getDayOfYear());

		StringBuilder batchControlNum = new StringBuilder(depositeDate.substring(0, 2)).append(yearDaysCount)
				.append(cashLetterSubString);

		return batchControlNum.toString();
	}
}
