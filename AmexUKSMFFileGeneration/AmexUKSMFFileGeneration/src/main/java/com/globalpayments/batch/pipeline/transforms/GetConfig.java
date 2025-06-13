package com.globalpayments.batch.pipeline.transforms;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.pipeline.exceptions.ConfigurationException;
import com.globalpayments.batch.pipeline.utils.Constants;
import com.globalpayments.batch.pipeline.utils.Utility;
import com.google.api.services.bigquery.model.TableRow;

/**
 * It creates a TableRow of configuration information to be used further in the program to run the
 job dynamically.
 * Connects to Cloud SQL by extracting the credentials from AppConfig file and gets the job configuration from
 etl_configuration table.
 */

public class GetConfig extends DoFn<String, TableRow> {

    private static final long serialVersionUID = 5440753035804589881L;

    public static final Logger LOG = LoggerFactory.getLogger(GetConfig.class);

    PCollectionView<List<String>> appConfig;
    CallableStatement statement;
    ResultSet resultSet;
    Connection connection;

    public GetConfig(PCollectionView<List<String>> appConfig) {
        this.appConfig = appConfig;
    }

    @ProcessElement
    public void processElement(ProcessContext c)
            throws SQLException, ConfigurationException {
        List<String> appConfigList = c.sideInput(appConfig);

        Map<String, String> map = new HashMap<>();
        String[] appConfigValues = null;

        if (null != appConfigList && !appConfigList.isEmpty()) {
            try {
                for (String str : appConfigList) {
                    appConfigValues = str.split(Constants.APP_CONFIG_DELIMITER);
                    if (null != appConfigValues && appConfigValues.length == 2)
                        map.put(appConfigValues[0].trim(), appConfigValues[1].trim());
                }

                connection = Utility.connectToCloudSql(map.get(Constants.URL), map.get(Constants.USERNAME),
                        map.get(Constants.PASSWORD));

                TableRow row = new TableRow();

                row.set(Constants.PROJECT, map.get(Constants.PROJECT));
                row.set(Constants.URL, map.get(Constants.URL));
                row.set(Constants.USERNAME, map.get(Constants.USERNAME));
                row.set(Constants.PASSWORD, map.get(Constants.PASSWORD));

                c.output(row);

            } catch (SQLException sql) {
                LOG.error(String.format("SQL Exception in GetConfig: %s", sql.toString()));
                throw new SQLException(String.format("SQL Exception in GetConfig: %s", sql.toString()));
            }  finally {
                if (null != connection && null != resultSet && null != statement) {
                    statement.close();
                    resultSet.close();
                    connection.close();
                }
            }
        } else {
            LOG.error("No elements in appConfig file");
            throw new ConfigurationException("Configuration Exception: No elements in appConfig file");
        }
    }
}