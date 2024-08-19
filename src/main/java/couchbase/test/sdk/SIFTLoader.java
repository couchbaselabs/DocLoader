package couchbase.test.sdk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import couchbase.test.docgen.DRConstants;
import couchbase.test.docgen.DocRange;
import couchbase.test.docgen.DocumentGenerator;
import couchbase.test.docgen.WorkLoadSettings;
import couchbase.test.loadgen.WorkLoadGenerate;
import couchbase.test.taskmanager.TaskManager;
import utils.common.FileDownload;

public class SIFTLoader {
    static Logger logger = LogManager.getLogger(SIFTLoader.class);
    static final String default_scope = "_default";
    static final String default_collection = "_default";

    public static void main(String[] args) throws IOException {

        logger.info("#################### Starting Java Based Doc-Loader ####################");

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

        Option scope = new Option("scope", true, "Scope");
        options.addOption(scope);

        Option collection = new Option("collection", true, "Collection");
        options.addOption(collection);

        Option port = new Option("p", "port", true, "Memcached Port");
        port.setRequired(true);
        options.addOption(port);

        Option create_s = new Option("create_s", "create_s", true, "Creates Start");
        options.addOption(create_s);

        Option create_e = new Option("create_e", "create_e", true, "Creates Start");
        options.addOption(create_e);

        Option read_s = new Option("read_s", "read_s", true, "Read Start");
        options.addOption(read_s);

        Option read_e = new Option("read_e", "read_e", true, "Read End");
        options.addOption(read_e);

        Option update_s = new Option("update_s", "update_s", true, "Update Start");
        options.addOption(update_s);

        Option update_e = new Option("update_e", "update_e", true, "Update End");
        options.addOption(update_e);

        Option delete_s = new Option("delete_s", "delete_s", true, "Delete Start");
        options.addOption(delete_s);

        Option delete_e = new Option("delete_e", "delete_e", true, "Delete End");
        options.addOption(delete_e);

        Option touch_s = new Option("touch_s", "touch_s", true, "Touch Start");
        options.addOption(touch_s);

        Option touch_e = new Option("touch_e", "touch_e", true, "Touch End");
        options.addOption(touch_e);

        Option replace_s = new Option("replace_s", "replace_s", true, "Replace Start");
        options.addOption(replace_s);

        Option replace_e = new Option("replace_e", "replace_w", true, "Replace End");
        options.addOption(replace_e);

        Option expiry_s = new Option("expiry_s", "expiry_s", true, "Expiry Start");
        options.addOption(expiry_s);

        Option expiry_e = new Option("expiry_e", "expiry_e", true, "Expiry End");
        options.addOption(expiry_e);

        Option create = new Option("cr", "create", true, "Creates%");
        options.addOption(create);

        Option update = new Option("up", "update", true, "Updates%");
        options.addOption(update);

        Option delete = new Option("dl", "delete", true, "Deletes%");
        options.addOption(delete);

        Option expiry = new Option("ex", "expiry", true, "Expiry%");
        options.addOption(expiry);

        Option read = new Option("rd", "read", true, "Reads%");
        options.addOption(read);

        Option workers = new Option("w", "workers", true, "Workers");
        options.addOption(workers);

        Option ops = new Option("ops", "ops", true, "Ops/Sec");
        options.addOption(ops);

        Option keySize = new Option("keySize", "keySize", true, "Size of the key");
        options.addOption(keySize);

        Option docSize = new Option("docSize", "docSize", true, "Size of the doc");
        options.addOption(docSize);

        Option loadType = new Option("loadType", "loadType", true, "Hot/Cold");
        options.addOption(loadType);

        Option keyType = new Option("keyType", "keyType", true, "Random/Sequential/Reverse");
        options.addOption(keyType);

        Option keyPrefix = new Option("keyPrefix", "keyPrefix", true, "String");
        options.addOption(keyPrefix);

        Option valueType = new Option("valueType", "valueType", true, "");
        options.addOption(valueType);

        Option validate = new Option("validate", "validate", true, "Validate Data during Reads");
        options.addOption(validate);

        Option gtm = new Option("gtm", "gtm", true, "Go for max doc ops");
        options.addOption(gtm);

        Option deleted = new Option("deleted", "deleted", true, "To verify deleted docs");
        options.addOption(deleted);

        Option base64 = new Option("base64", "base64", true, "base64 encoding for Vector embedding");
        options.addOption(base64);

        Option durability = new Option("durability", true, "Durability Level");
        options.addOption(durability);

        Option mutate = new Option("mutate", true, "mutate");
        options.addOption(mutate);
        
        options.addOption(new Option("baseVectorsFilePath", true, "baseVectorsFilePath"));

        options.addOption(new Option("siftURL", true, "siftURL"));

        Option retry = new Option("retry", true, "Retry failures n times");
        options.addOption(retry);

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

        String baseVectorsFilePath = cmd.getOptionValue("baseVectorsFilePath", null);
        String siftURL = cmd.getOptionValue("siftURL",null);
        String siftFileName = Paths.get(baseVectorsFilePath, "bigann_base.bvecs").toString();
        File fh = new File(siftFileName);
        if(!fh.exists()) {
            String siftFileNameZip = Paths.get(baseVectorsFilePath, Paths.get(siftURL).getFileName().toString()).toString();
            if(! new File(siftFileNameZip).exists()) {
                Files.createDirectories(Paths.get(baseVectorsFilePath));
                FileDownload.downloadWithJavaIO(siftURL, siftFileNameZip);
            } else {
                logger.info(String.format("%s Found!! Unzipping it.", siftFileNameZip));
            }
            SIFTLoader.decompressGzip(
                    Paths.get(siftFileNameZip),
                    Paths.get(siftFileName)
                    );
            logger.info(String.format("Unzipping %s completed. %s is ready to use.", siftFileNameZip, siftFileName));
        } else {
            logger.info(siftFileName + " Found!!");
        }

        Server master = new Server(cmd.getOptionValue("node"), cmd.getOptionValue("port"),
                cmd.getOptionValue("rest_username"), cmd.getOptionValue("rest_password"), cmd.getOptionValue("port"));
        TaskManager tm = new TaskManager(Integer.parseInt(cmd.getOptionValue("workers", "10")));
        SDKClient client = new SDKClient(master, cmd.getOptionValue("bucket"), cmd.getOptionValue("scope", "_default"),
                cmd.getOptionValue("collection", "_default"));
        try {
            client.initialiseSDK();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int[] steps = new int[] {0, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000, 100000000, 200000000, 500000000, 1000000000};
        int poolSize = Integer.parseInt(cmd.getOptionValue("workers", "10"));
        int until = Integer.parseInt(cmd.getOptionValue(DRConstants.create_e, "0"));
        for (int k = 0; k < steps.length - 1 && until > steps[k]; k++) {
            for (int i = 0; i < poolSize; i++) {
                WorkLoadSettings ws = new WorkLoadSettings(cmd.getOptionValue("keyPrefix", "test_docs-"),
                        Integer.parseInt(cmd.getOptionValue("keySize", "20")),
                        Integer.parseInt(cmd.getOptionValue("docSize", "256")),
                        Integer.parseInt(cmd.getOptionValue("cr", "0")), Integer.parseInt(cmd.getOptionValue("rd", "0")),
                        Integer.parseInt(cmd.getOptionValue("up", "0")), Integer.parseInt(cmd.getOptionValue("dl", "0")),
                        Integer.parseInt(cmd.getOptionValue("ex", "0")), Integer.parseInt(cmd.getOptionValue("workers", "10")),
                        Integer.parseInt(cmd.getOptionValue("ops", "10000")), cmd.getOptionValue("loadType", null),
                        cmd.getOptionValue("keyType", "SimpleKey"), "siftBigANN",
                        Boolean.parseBoolean(cmd.getOptionValue("validate", "false")),
                        Boolean.parseBoolean(cmd.getOptionValue("gtm", "false")),
                        Boolean.parseBoolean(cmd.getOptionValue("deleted", "false")),
                        Integer.parseInt(cmd.getOptionValue("mutate", "0")),
                        Boolean.parseBoolean(cmd.getOptionValue("elastic", "false")),
                        cmd.getOptionValue("model", "sentence-transformers/paraphrase-MiniLM-L3-v2"),
                        Boolean.parseBoolean(cmd.getOptionValue("mockVector", "false")),
                        Integer.parseInt(cmd.getOptionValue("dim", "0")),
                        Boolean.parseBoolean(cmd.getOptionValue("base64", "false")),
                        cmd.getOptionValue("mutate_field",""),
                        Integer.parseInt(cmd.getOptionValue("mutation_timeout","0")),
                        siftFileName);
                int step = (steps[k+1] - steps[k])/poolSize;
                int start = steps[k] + step * i;
                int end = steps[k] + step * (i+1);
                HashMap<String, Number> dr = new HashMap<String, Number>();
                dr.put(DRConstants.create_s, start);
                dr.put(DRConstants.create_e, end);
                dr.put(DRConstants.read_s, Long.parseLong(cmd.getOptionValue(DRConstants.read_s, "0")));
                dr.put(DRConstants.read_e, Long.parseLong(cmd.getOptionValue(DRConstants.read_e, "0")));
                dr.put(DRConstants.update_s, Long.parseLong(cmd.getOptionValue(DRConstants.update_s, "0")));
                dr.put(DRConstants.update_e, Long.parseLong(cmd.getOptionValue(DRConstants.update_e, "0")));
                dr.put(DRConstants.delete_s, Long.parseLong(cmd.getOptionValue(DRConstants.delete_s, "0")));
                dr.put(DRConstants.delete_e, Long.parseLong(cmd.getOptionValue(DRConstants.delete_e, "0")));
                dr.put(DRConstants.touch_s, Long.parseLong(cmd.getOptionValue(DRConstants.touch_s, "0")));
                dr.put(DRConstants.touch_e, Long.parseLong(cmd.getOptionValue(DRConstants.touch_e, "0")));
                dr.put(DRConstants.replace_s, Long.parseLong(cmd.getOptionValue(DRConstants.replace_s, "0")));
                dr.put(DRConstants.replace_e, Long.parseLong(cmd.getOptionValue(DRConstants.replace_e, "0")));
                dr.put(DRConstants.expiry_s, Long.parseLong(cmd.getOptionValue(DRConstants.expiry_s, "0")));
                dr.put(DRConstants.expiry_e, Long.parseLong(cmd.getOptionValue(DRConstants.expiry_e, "0")));

                DocRange range = new DocRange(dr);
                ws.dr = range;
                DocumentGenerator dg = null;
                try {
                    dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
                } catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }
                try {
                    String th_name = "Loader_" + k + "_" + ws.dr.create_s + "_" + ws.dr.create_e;
                    boolean trackFailures = false;
                    if (Integer.parseInt(cmd.getOptionValue("retry", "0")) > 0)
                        trackFailures = true;
                    tm.submit(new WorkLoadGenerate(th_name, dg, client, null, cmd.getOptionValue("durability", "NONE"),
                            Integer.parseInt(cmd.getOptionValue("maxTTL", "0")),
                            cmd.getOptionValue("maxTTLUnit", "seconds"), trackFailures,
                            Integer.parseInt(cmd.getOptionValue("retry", "0")), null));
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        tm.getAllTaskResult();
        tm.shutdown();
        client.disconnectCluster();
        client.shutdownEnv();
    }

    public static void decompressGzip(Path source, Path target) throws IOException {

        try (GZIPInputStream gis = new GZIPInputStream(
                                      new FileInputStream(source.toFile()));
             FileOutputStream fos = new FileOutputStream(target.toFile())) {

            // copy GZIPInputStream to FileOutputStream
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }

        }

    }
}
