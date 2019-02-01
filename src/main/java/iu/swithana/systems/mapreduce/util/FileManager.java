package iu.swithana.systems.mapreduce.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileManager {

    public List<File> getFilesInDirectory(String folderPath) {
        return new ArrayList<>(Arrays.asList(new File(folderPath).listFiles()));
    }

    public byte[] readFile(File file) throws IOException {
        if (file.isFile()) {
            return Files.readAllBytes(file.toPath());
        } else {
            return null;
        }
    }
}
