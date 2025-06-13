package com.globalpayments.batch.pipeline.transforms;


import com.globalpayments.batch.pipeline.exceptions.ConfigurationException;
import com.globalpayments.batch.pipeline.exceptions.ConnectionFailureException;
import com.globalpayments.batch.pipeline.exceptions.SimpleBatchException;
import com.globalpayments.batch.pipeline.utils.Constants;
import com.globalpayments.batch.pipeline.utils.Utility;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
/**
 * Updates the Audit table in CLoud SQL
 * Calls a procedure to set the Source_Count
 */

public class UpdateAmexUKPendingConfig extends DoFn<String,String>
{
    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(UpdateAmexUKPendingConfig.class);
    PCollectionView<TableRow> config;
    PCollectionView<Long> fileRecordsCount;
    ValueProvider<String> outputFilePath;

    Connection connection;
    CallableStatement statement;
    String jdbcUrl;
    String user;
    String pass;

    public UpdateAmexUKPendingConfig(PCollectionView<TableRow> config,
                                     ValueProvider<String> outputFilePath,
                                     PCollectionView<Long> fileRecordsCount)
    {
        this.config = config;
        this.outputFilePath = outputFilePath;
        this.fileRecordsCount = fileRecordsCount;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws SQLException, ConnectionFailureException, SimpleBatchException, ConfigurationException
    {
        try {
            TableRow configuration = c.sideInput(config);
            Long fileCount = c.sideInput(fileRecordsCount);
            String outputFile = outputFilePath.get();


            if (null != configuration && null != configuration.get(Constants.URL) && null != configuration.get(Constants.USERNAME)
                    && null != configuration.get(Constants.PASSWORD)) {

                jdbcUrl = configuration.get(Constants.URL).toString();
                user = configuration.get(Constants.USERNAME).toString();
                pass = configuration.get(Constants.PASSWORD).toString();
                connection = Utility.connectToCloudSql(jdbcUrl, user, pass);

                if (null != connection) {
                    statement = connection.prepareCall("{call updateAmexUKPendingConfig(?,?)}");
                    statement.setString(1, outputFile);
                    statement.setLong(2, fileCount);
                    statement.executeUpdate();
                } else {
                    LOG.error("Failed to connect to Cloud SQL in updateAmexUKPendingConfig: Connection is null");
                    throw new ConnectionFailureException("Failed to connect to Cloud SQL");
                }
            } else
                throw new ConfigurationException("Configuration Exception in updateAmexUKPendingConfig");
        } catch (SQLException sql) {
            LOG.error(String.format("SQL Exception in updateAmexUKPendingConfig: %s", sql.toString()));
            throw new SQLException("SQL Exception During updateAmexUKPendingConfig : " + sql.toString());
        } catch (ConfigurationException ce) {
            LOG.error(String.format("Configuration Exception in updateAmexUKPendingConfig: %s", ce.toString()));
            throw new ConfigurationException("Configuration Exception During updateAmexUKPendingConfig : " + ce.toString());
        } catch (ConnectionFailureException cf) {
            LOG.error(String.format("Connection Failure Exception in updateAmexUKPendingConfig: %s", cf.toString()));
            throw new ConnectionFailureException(String.format("Connection Failure in updateAmexUKPendingConfig: %s", cf.toString()));
        } catch (Exception e) {
            LOG.error(String.format("Exception During updateAmexUKPendingConfig : %s", e.toString()));
            throw new SimpleBatchException("Simple Batch Exception in updateAmexUKPendingConfig");
        } finally {
            if (null != connection && null != statement) {
                connection.close();
                statement.close();
            }
        }

    }
}
