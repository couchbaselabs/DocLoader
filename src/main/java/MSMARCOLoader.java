
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import couchbase.loadgen.WorkLoadGenerate;
import couchbase.sdk.SDKClientPool;
import couchbase.sdk.Server;
import elasticsearch.EsClient;
import utils.docgen.DRConstants;
import utils.docgen.DocRange;
import utils.docgen.DocumentGenerator;
import utils.docgen.WorkLoadSettings;
import utils.taskmanager.TaskManager;
import utils.val.MSMARCOEmbeddingProduct;
import utils.val.MSMARCOSiftEmbeddingProduct;

public class MSMARCOLoader {
    static Logger logger = LogManager.getLogger(MSMARCOLoader.class);

    public static void main(String[] args) throws IOException {
        logger.info("#################### Starting MSMARCO Sparse Vector Loader ####################");

        Options options = new Options();

        Option name = new Option("n", "node", true, "IP Address");
        name.setRequired(true);
        options.addOption(name);

        Option rest_username = new Option("user", "rest_username", true, "Username");
        rest_username.setRequired(true);
        options.addOption(rest_username);

        Option rest_password = new Option("pwd", "rest_password", true, "Password");
        rest_password.setRequired(true);
        options.addOption(rest_password);

        Option bucket = new Option("b", "bucket", true, "Bucket");
        bucket.setRequired(true);
        options.addOption(bucket);

        Option port = new Option("p", "port", true, "Memcached Port");
        port.setRequired(true);
        options.addOption(port);

        options.addOption(new Option("scope", true, "Scope"));
        options.addOption(new Option("collection", true, "Collection"));

        options.addOption(new Option("create_s", "create_s", true, "Creates Start"));
        options.addOption(new Option("create_e", "create_e", true, "Creates End"));
        options.addOption(new Option("read_s", "read_s", true, "Read Start"));
        options.addOption(new Option("read_e", "read_e", true, "Read End"));
        options.addOption(new Option("update_s", "update_s", true, "Update Start"));
        options.addOption(new Option("update_e", "update_e", true, "Update End"));
        options.addOption(new Option("delete_s", "delete_s", true, "Delete Start"));
        options.addOption(new Option("delete_e", "delete_e", true, "Delete End"));
        options.addOption(new Option("touch_s", "touch_s", true, "Touch Start"));
        options.addOption(new Option("touch_e", "touch_e", true, "Touch End"));
        options.addOption(new Option("replace_s", "replace_s", true, "Replace Start"));
        options.addOption(new Option("replace_e", "replace_e", true, "Replace End"));
        options.addOption(new Option("expiry_s", "expiry_s", true, "Expiry Start"));
        options.addOption(new Option("expiry_e", "expiry_e", true, "Expiry End"));

        options.addOption(new Option("cr", "create", true, "Creates%"));
        options.addOption(new Option("up", "update", true, "Updates%"));
        options.addOption(new Option("dl", "delete", true, "Deletes%"));
        options.addOption(new Option("ex", "expiry", true, "Expiry%"));
        options.addOption(new Option("rd", "read", true, "Reads%"));

        options.addOption(new Option("w", "workers", true, "Workers"));
        options.addOption(new Option("ops", "ops", true, "Ops/Sec"));
        options.addOption(new Option("keySize", "keySize", true, "Size of the key"));
        options.addOption(new Option("docSize", "docSize", true, "Size of the doc"));
        options.addOption(new Option("loadType", "loadType", true, "Hot/Cold"));
        options.addOption(new Option("keyType", "keyType", true, "Random/Sequential/Reverse"));
        options.addOption(new Option("keyPrefix", "keyPrefix", true, "String"));
        options.addOption(new Option("validate", "validate", true, "Validate Data during Reads"));
        options.addOption(new Option("gtm", "gtm", true, "Go for max doc ops"));
        options.addOption(new Option("deleted", "deleted", true, "To verify deleted docs"));
        options.addOption(new Option("base64", "base64", true, "base64 encoding for Vector embedding"));
        options.addOption(new Option("durability", true, "Durability Level"));
        options.addOption(new Option("mutate", true, "mutate"));
        options.addOption(new Option("mutation_timeout", true, "Mutation timeout in seconds"));
        options.addOption(new Option("mutate_field", true, "Mutate field"));
        options.addOption(new Option("maxTTL", true, "Expiry Time"));
        options.addOption(new Option("maxTTLUnit", true, "Expiry Time unit"));
        options.addOption(new Option("retry", true, "Retry failures n times"));

        options.addOption(new Option("vecFilePath", true, "Path to the .vec sparse vector file"));
        options.addOption(new Option("siftFilePath", true, "Path to SIFT bigann_base.bvecs file (required for MSMARCOSiftEmbeddingProduct)"));
        options.addOption(new Option("valueType", true, "Value type to generate (default MSMARCOEmbeddingProduct)"));

        Option elastic = new Option("elastic", "elastic", true, "Flag to insert data in ElasticSearch cluster");
        options.addOption(elastic);
        Option esServer = new Option("esServer", "esServer", true, "ElasticSearch cluster");
        options.addOption(esServer);
        Option esAPIKey = new Option("esAPIKey", "esAPIKey", true, "ElasticSearch APIKey");
        options.addOption(esAPIKey);
        Option esSimilarity = new Option("esSimilarity", "esSimilarity", true, "ElasticSearch esSimilarity");
        options.addOption(esSimilarity);
        Option skipCB = new Option("skipCB", "skipCB", true, "Skip loading data into Couchbase");
        options.addOption(skipCB);

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            formatter.printHelp("Supported Options", options);
            System.exit(1);
            return;
        }

        String vecFilePath = cmd.getOptionValue("vecFilePath");
        if (vecFilePath == null || vecFilePath.trim().isEmpty()) {
            System.err.println("Error: -vecFilePath is required (path to .vec sparse vector file)");
            System.exit(1);
            return;
        }
        String valueType = cmd.getOptionValue("valueType", MSMARCOEmbeddingProduct.class.getSimpleName());
        String siftFilePath = cmd.getOptionValue("siftFilePath");
        if (MSMARCOSiftEmbeddingProduct.class.getSimpleName().equals(valueType)
                && (siftFilePath == null || siftFilePath.trim().isEmpty())) {
            System.err.println("Error: -siftFilePath is required when -valueType is MSMARCOSiftEmbeddingProduct");
            System.exit(1);
            return;
        }

        Server master = new Server(cmd.getOptionValue("node"), cmd.getOptionValue("port"),
                cmd.getOptionValue("rest_username"), cmd.getOptionValue("rest_password"), cmd.getOptionValue("port"));
        TaskManager tm = new TaskManager(Integer.parseInt(cmd.getOptionValue("workers", "10")));
        SDKClientPool clientPool = new SDKClientPool();
        String cb = cmd.getOptionValue("skipCB", "false");
        if (!Boolean.parseBoolean(cb)) {
            try {
                clientPool.create_clients(cmd.getOptionValue("bucket"), master, 2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        EsClient esClient = null;
        if (Boolean.parseBoolean(cmd.getOptionValue("elastic", "false"))) {
            if (cmd.getOptionValue("esAPIKey") != null)
                esClient = new EsClient(cmd.getOptionValue("esServer"), cmd.getOptionValue("esAPIKey"));
            if (esClient != null) {
                esClient.initializeSDK();
                esClient.deleteESIndex(cmd.getOptionValue("collection", "_default").replace("_", ""));
                esClient.createESIndex(cmd.getOptionValue("collection", "_default").replace("_", ""),
                        cmd.getOptionValue("esSimilarity", "l2_norm"), null);
            }
        }

        // Use the same step ranges as MSMARCOEmbeddingProduct
        long[] steps = MSMARCOEmbeddingProduct.getSteps();
        int poolSize = Integer.parseInt(cmd.getOptionValue("workers", "10"));
        long start_offset = 0, end_offset = 0;
        if (Integer.parseInt(cmd.getOptionValue("cr", "0")) > 0) {
            start_offset = Long.parseLong(cmd.getOptionValue(DRConstants.create_s, "0"));
            end_offset = Long.parseLong(cmd.getOptionValue(DRConstants.create_e, "0"));
        } else if (Integer.parseInt(cmd.getOptionValue("up", "0")) > 0) {
            start_offset = Long.parseLong(cmd.getOptionValue(DRConstants.update_s, "0"));
            end_offset = Long.parseLong(cmd.getOptionValue(DRConstants.update_e, "0"));
        } else if (Integer.parseInt(cmd.getOptionValue("ex", "0")) > 0) {
            start_offset = Long.parseLong(cmd.getOptionValue(DRConstants.expiry_s, "0"));
            end_offset = Long.parseLong(cmd.getOptionValue(DRConstants.expiry_e, "0"));
        }

        // Find which step range contains start_offset
        int k = 0;
        while (!(steps[k] <= start_offset && start_offset < steps[k + 1]))
            k += 1;

        // Process each step range that overlaps with [start_offset, end_offset)
        while (steps[k] < end_offset) {
            long start = Math.max(start_offset, steps[k]);
            long end = Math.min(end_offset, steps[k + 1]);
            long step = (end - start) / poolSize;

            for (int i = 0; i < poolSize; i++) {
                WorkLoadSettings ws = new WorkLoadSettings(
                        cmd.getOptionValue("keyPrefix", "msmarco-"),
                        Integer.parseInt(cmd.getOptionValue("keySize", "20")),
                        Integer.parseInt(cmd.getOptionValue("docSize", "256")),
                        Integer.parseInt(cmd.getOptionValue("cr", "0")),
                        Integer.parseInt(cmd.getOptionValue("rd", "0")),
                        Integer.parseInt(cmd.getOptionValue("up", "0")),
                        Integer.parseInt(cmd.getOptionValue("dl", "0")),
                        Integer.parseInt(cmd.getOptionValue("ex", "0")),
                        Integer.parseInt(cmd.getOptionValue("workers", "10")),
                        Integer.parseInt(cmd.getOptionValue("ops", "10000")),
                        cmd.getOptionValue("loadType", null),
                        cmd.getOptionValue("keyType", "SimpleKey"),
                        valueType,
                        Boolean.parseBoolean(cmd.getOptionValue("validate", "false")),
                        Boolean.parseBoolean(cmd.getOptionValue("gtm", "false")),
                        Boolean.parseBoolean(cmd.getOptionValue("deleted", "false")),
                        Integer.parseInt(cmd.getOptionValue("mutate", "0")),
                        Boolean.parseBoolean(cmd.getOptionValue("elastic", "false")),
                        cmd.getOptionValue("model", ""),
                        false,
                        0,
                        Boolean.parseBoolean(cmd.getOptionValue("base64", "false")),
                        cmd.getOptionValue("mutate_field", ""),
                        Integer.parseInt(cmd.getOptionValue("mutation_timeout", "0")),
                        vecFilePath);
                ws.embeddingFilePath = vecFilePath;
                ws.baseVectorsFilePath = MSMARCOSiftEmbeddingProduct.class.getSimpleName().equals(valueType)
                        ? siftFilePath
                        : vecFilePath;

                long workerStart = start + step * i;
                long workerEnd = (i == poolSize - 1) ? end : start + step * (i + 1);
                HashMap<String, Number> dr = new HashMap<String, Number>();
                dr.put(DRConstants.create_s, workerStart);
                dr.put(DRConstants.create_e, workerEnd);
                dr.put(DRConstants.read_s, Long.parseLong(cmd.getOptionValue(DRConstants.read_s, "0")));
                dr.put(DRConstants.read_e, Long.parseLong(cmd.getOptionValue(DRConstants.read_e, "0")));
                dr.put(DRConstants.update_s, workerStart);
                dr.put(DRConstants.update_e, workerEnd);
                dr.put(DRConstants.delete_s, Long.parseLong(cmd.getOptionValue(DRConstants.delete_s, "0")));
                dr.put(DRConstants.delete_e, Long.parseLong(cmd.getOptionValue(DRConstants.delete_e, "0")));
                dr.put(DRConstants.touch_s, Long.parseLong(cmd.getOptionValue(DRConstants.touch_s, "0")));
                dr.put(DRConstants.touch_e, Long.parseLong(cmd.getOptionValue(DRConstants.touch_e, "0")));
                dr.put(DRConstants.replace_s, Long.parseLong(cmd.getOptionValue(DRConstants.replace_s, "0")));
                dr.put(DRConstants.replace_e, Long.parseLong(cmd.getOptionValue(DRConstants.replace_e, "0")));
                dr.put(DRConstants.expiry_s, workerStart);
                dr.put(DRConstants.expiry_e, workerEnd);

                DocRange range = new DocRange(dr);
                ws.dr = range;
                DocumentGenerator dg = null;
                try {
                    dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
                } catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }
                try {
                    String th_name = "MSMARCOLoader_" + k + "_" + ws.dr.create_s + "_" + ws.dr.create_e;
                    boolean trackFailures = false;
                    if (Integer.parseInt(cmd.getOptionValue("retry", "0")) > 0)
                        trackFailures = true;
                    WorkLoadGenerate wlg = new WorkLoadGenerate(th_name, dg, clientPool, esClient,
                            cmd.getOptionValue("durability", "NONE"),
                            Integer.parseInt(cmd.getOptionValue("maxTTL", "0")),
                            cmd.getOptionValue("maxTTLUnit", "seconds"), trackFailures,
                            Integer.parseInt(cmd.getOptionValue("retry", "0")), null);
                    wlg.set_collection_for_load(
                            cmd.getOptionValue("bucket"),
                            cmd.getOptionValue("scope", "_default"),
                            cmd.getOptionValue("collection", "_default"));
                    tm.submit(wlg);
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            k += 1;
        }
        tm.getAllTaskResult();
        tm.shutdown();
        if (esClient != null)
            esClient.transport.close();
    }
}
