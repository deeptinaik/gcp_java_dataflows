package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

import com.google.api.services.bigquery.model.TableRow;

public class CleaningData extends DoFn<TableRow, TableRow> {

	private static final long serialVersionUID = -4578103281700104167L;

	@ProcessElement
	public void processElement(ProcessContext c) {
		TableRow tableRow = c.element();
		System.out.println("Beforetable "+tableRow);
		//tableRow.set("alpha_currency_code", tableRow.get("updated_currency_code"));
		tableRow.remove("dmi_merchant_number");
		tableRow.remove("dmi_corporate");
		tableRow.remove("dmi_region");
		tableRow.remove("dmi_corporate_region");
		tableRow.remove("ddc_corporate_region");
		tableRow.remove("ddc_currency_code");
		tableRow.remove("dincc_currency_code");
		tableRow.remove("dincc_iso_numeric_currency_code");
		tableRow.remove("updated_currency_code");
		tableRow.remove("dincc_iso_numeric_currency_code_sk");
		System.out.println("Aftertable "+tableRow);
		
		c.output(tableRow);
	}

}

