package iu.swithana.systems.mapreduce.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileManager {

    public File[] getFilesInDirectory(String folderPath) {
        return new File(folderPath).listFiles();
    }

    public byte[] readFile(File file) throws IOException {
        if (file.isFile()) {
            return Files.readAllBytes(file.toPath());
        } else {
            return null;
        }
    }
}
