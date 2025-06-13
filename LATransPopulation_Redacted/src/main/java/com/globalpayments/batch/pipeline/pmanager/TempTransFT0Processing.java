package com.globalpayments.batch.pipeline.pmanager;

import java.util.Map;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.globalpayments.batch.pipeline.transforms.TransFT0FieldLookUp;
import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.UniqueKeyFunction;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TempTransFT0Processing extends PTransform<PBegin, PCollection<TableRow>> implements CommonUtil {
	private static final long serialVersionUID = -2048921308624628365L;

	PCollection<TableRow> trustedTransFt;
	String[] dimCorporateFields = { CORPORATE, CORPORATE_SK };
	String[] dimRegionFields = { REGION, CORPORATE_SK, REGION_SK };
	String[] dimPrincipalFields = { PRINCIPAL, CORPORATE_SK, REGION_SK, PRINCIPAL_SK };
	String[] dimAssociateFields = { ASSOCIATE, CORPORATE_SK, REGION_SK, PRINCIPAL_SK, ASSOCIATE_SK };
	String[] dimChainFields = { CHAIN, CORPORATE_SK, REGION_SK, PRINCIPAL_SK, ASSOCIATE_SK, CHAIN_SK };

	public TempTransFT0Processing(PCollection<TableRow> trustedTransFt)
	{
		this.trustedTransFt = trustedTransFt;
	}


	@Override
	public PCollection<TableRow> expand(PBegin pBegin) 
	{
		String[] dimCorpKey = { CORPORATE };
		String[] dimRegionKey = { CORPORATE_SK, REGION };
		String[] dimPrincipalKey = { CORPORATE_SK, REGION_SK, PRINCIPAL };
		String[] dimAssociateKey = { CORPORATE_SK, REGION_SK, PRINCIPAL_SK, ASSOCIATE };
		String[] dimChainKey = { CORPORATE_SK, REGION_SK, PRINCIPAL_SK, ASSOCIATE_SK, CHAIN };
	
		PCollectionView<Map<String, TableRow>> dimCorporateView = fetchTableInMap(pBegin, DIM_CORPORATE, dimCorpKey,
				dimCorporateFields);

		PCollectionView<Map<String, TableRow>> dimRegionView = fetchTableInMap(pBegin, DIM_REGION, dimRegionKey,
				dimRegionFields);

		PCollectionView<Map<String, TableRow>> dimPrincipalView = fetchTableInMap(pBegin, DIM_PRINCIPAL,
				dimPrincipalKey, dimPrincipalFields);

		PCollectionView<Map<String, TableRow>> dimAssociateView = fetchTableInMap(pBegin, DIM_ASSOCIATE,
				dimAssociateKey, dimAssociateFields);

		PCollectionView<Map<String, TableRow>> dimChainView = fetchTableInMap(pBegin, DIM_CHAIN, dimChainKey,
				dimChainFields);
		
		
		return trustedTransFt.apply("TransFt0 lookup",
				ParDo.of(new TransFT0FieldLookUp(dimCorporateView, dimRegionView, dimPrincipalView, dimAssociateView,
						dimChainView)).withSideInputs(dimCorporateView, dimRegionView, dimPrincipalView,
								dimAssociateView, dimChainView));
		
		
	}


	public PCollectionView<Map<String, TableRow>> fetchTableInMap(PBegin pBegin, String dimentionTableName,
			String[] lookUpKeys, String[] filterFields)
	{

		PCollection<KV<String, TableRow>> sourceTableData = Utility.fetchTableRows(pBegin, dimentionTableName,
				String.format("Reading from %s table", dimentionTableName), filterFields, lookUpKeys)
				.apply(Distinct.withRepresentativeValueFn(new UniqueKeyFunction()))
				.setCoder(KvCoder.<String, TableRow>of(StringUtf8Coder.of(), TableRowJsonCoder.of()));

		return sourceTableData.apply("create view", View.asMap());
	}
}
