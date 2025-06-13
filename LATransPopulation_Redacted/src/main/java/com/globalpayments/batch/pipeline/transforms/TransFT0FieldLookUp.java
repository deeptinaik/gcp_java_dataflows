package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollectionView;

import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.google.api.services.bigquery.model.TableRow;

public class TransFT0FieldLookUp extends  DoFn<TableRow, TableRow> implements CommonUtil {
	private static final long serialVersionUID = 4136091013549293045L;

	PCollectionView<Map<String, TableRow>> dimCorporateView;
	PCollectionView<Map<String, TableRow>> dimRegionView;
	PCollectionView<Map<String, TableRow>> dimPrincipalView;
	PCollectionView<Map<String, TableRow>> dimAssociateView;
	PCollectionView<Map<String, TableRow>> dimChainView;

	public TransFT0FieldLookUp(PCollectionView<Map<String, TableRow>> dimCorporateView,
			PCollectionView<Map<String, TableRow>> dimRegionView,
			PCollectionView<Map<String, TableRow>> dimPrincipalView,
			PCollectionView<Map<String, TableRow>> dimAssociateView,
			PCollectionView<Map<String, TableRow>> dimChainView) {

		this.dimCorporateView = dimCorporateView;
		this.dimRegionView = dimRegionView;
		this.dimPrincipalView = dimPrincipalView;
		this.dimAssociateView = dimAssociateView;
		this.dimChainView = dimChainView;
	}

	@ProcessElement
	public void processElement(ProcessContext processContext) {

		Map<String, TableRow> dimCorporatMap = processContext.sideInput(dimCorporateView);
		Map<String, TableRow> dimRegionMap = processContext.sideInput(dimRegionView);
		Map<String, TableRow> dimPrincipalMap = processContext.sideInput(dimPrincipalView);
		Map<String, TableRow> dimAssociateMap = processContext.sideInput(dimAssociateView);
		Map<String, TableRow> dimChainMap = processContext.sideInput(dimChainView);

		TableRow outputTableRow = processContext.element().clone();
	
		// corporate_sk lookup
				if (null != processContext.element().get(CORPORATE))
				{
					outputTableRow.set(CORPORATE_SK,
							null != dimCorporatMap.get(processContext.element().get(CORPORATE))
									? dimCorporatMap.get(processContext.element().get(CORPORATE)).get(CORPORATE_SK): DMX_LOOKUP_FAILURE);
				}
				else
				{
					outputTableRow.set(CORPORATE_SK , DMX_LOOKUP_NULL_BLANK);
				}
		

  	// region_sk lookup
				if(null != outputTableRow.get(REGION))
				{
				StringBuilder regionKey = new StringBuilder(outputTableRow.get(CORPORATE_SK).toString())
						.append(outputTableRow.get(REGION).toString());
					
				outputTableRow.set(REGION_SK, dimRegionMap.containsKey(regionKey.toString())
							? dimRegionMap.get(regionKey.toString()).get(REGION_SK) : DMX_LOOKUP_FAILURE);
				}
				else
				{
					outputTableRow.set(REGION_SK,DMX_LOOKUP_NULL_BLANK);
				}
				

		// principal_sk lookup
				if(null != outputTableRow.get(PRINCIPAL))
				{
					StringBuilder principalKey = new StringBuilder(outputTableRow.get(CORPORATE_SK).toString())
							.append(outputTableRow.get(REGION_SK).toString())
							.append(outputTableRow.get(PRINCIPAL).toString());
					outputTableRow.set(PRINCIPAL_SK, dimPrincipalMap.containsKey(principalKey.toString())
							? dimPrincipalMap.get(principalKey.toString()).get(PRINCIPAL_SK) : DMX_LOOKUP_FAILURE);
				}
				else
				{
					outputTableRow.set(PRINCIPAL_SK,DMX_LOOKUP_NULL_BLANK);
				}
				

				// associate_sk lookup
				if(null != outputTableRow.get(ASSOCIATE)){
					StringBuilder associateKey = new StringBuilder(outputTableRow.get(CORPORATE_SK).toString())
							.append(outputTableRow.get(REGION_SK).toString()).append(outputTableRow.get(PRINCIPAL_SK))
							.append(outputTableRow.get(ASSOCIATE).toString());
					outputTableRow.set(ASSOCIATE_SK, dimAssociateMap.containsKey(associateKey.toString())
							? dimAssociateMap.get(associateKey.toString()).get(ASSOCIATE_SK) : DMX_LOOKUP_FAILURE);
				}else{
					outputTableRow.set(ASSOCIATE_SK,DMX_LOOKUP_NULL_BLANK);
				}
				
				// chain_sk lookup
				if(null != outputTableRow.get(CHAIN)){
					StringBuilder chainKey = new StringBuilder(outputTableRow.get(CORPORATE_SK).toString())
							.append(outputTableRow.get(REGION_SK).toString()).append(outputTableRow.get(PRINCIPAL_SK))
							.append(outputTableRow.get(ASSOCIATE_SK))
							.append(outputTableRow.get(CHAIN).toString());
					outputTableRow.set(CHAIN_SK, dimChainMap.containsKey(chainKey.toString())
							? dimChainMap.get(chainKey.toString()).get(CHAIN_SK) : DMX_LOOKUP_FAILURE);
				}else{
					outputTableRow.set(CHAIN_SK,DMX_LOOKUP_NULL_BLANK);
				}
				
				processContext.output(outputTableRow);
			}
		}
