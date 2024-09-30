package utils.common;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FileDownload {
	static Logger logger = LogManager.getLogger(FileDownload.class);

	public static void checkDownload(String baseVectorsFilePath, String siftURL) throws IOException {
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
            FileDownload.decompressGzip(
                    Paths.get(siftFileNameZip),
                    Paths.get(siftFileName)
                    );
            logger.info(String.format("Unzipping %s completed. %s is ready to use.", siftFileNameZip, siftFileName));
        } else {
            logger.info(siftFileName + " Found!!");
        }
	}

    public static void downloadWithJavaIO(String url, String localFilename) {

        try (BufferedInputStream in = new BufferedInputStream(new URI(url).toURL().openStream()); FileOutputStream fileOutputStream = new FileOutputStream(localFilename)) {

            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException |URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void downloadWithJava7IO(String url, String localFilename) {

        try (InputStream in = new URI(url).toURL().openStream()) {
            Files.copy(in, Paths.get(localFilename), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void downloadWithJavaNIO(String fileURL, String localFilename) throws IOException, URISyntaxException {

        URL url = new URI(fileURL).toURL();
        try (ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream()); 
            FileOutputStream fileOutputStream = new FileOutputStream(localFilename); FileChannel fileChannel = fileOutputStream.getChannel()) {

            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            fileOutputStream.close();
        }
    }

    public static void downloadWithApacheCommons(String url, String localFilename) {

        int CONNECT_TIMEOUT = 10000;
        int READ_TIMEOUT = 10000;
        try {
            FileUtils.copyURLToFile(new URI(url).toURL(), new File(localFilename), CONNECT_TIMEOUT, READ_TIMEOUT);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

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