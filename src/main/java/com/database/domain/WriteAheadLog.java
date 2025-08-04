package com.database.domain;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WriteAheadLog {
    private static final Pattern PATTERN = Pattern.compile("^log_(\\d+)\\.txt$");

    private final File storeDirectory;
    private final ReadOnlyLogicalTimeProvider logicalTimeProvider;
    private File writeAheadFile;
    private FileWriter writer;

	public WriteAheadLog(String dir, ReadOnlyLogicalTimeProvider logicalTimeProvider) {
        this.logicalTimeProvider = logicalTimeProvider;

        storeDirectory = new File(dir + "/LOG");
		if (!storeDirectory.exists() || !storeDirectory.isDirectory())
            storeDirectory.mkdirs();
    }

    public void append(String data) {
        try {
            writer.append(data + "\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void newLogFile() {
        writeAheadFile = createNewWal();
        adjustFileWriter();
    }

    public List<Operation> getUncommitedOperations() {
        try {
            File[] savedWriteAheadLogFiles =
                    findFiles(storeDirectory.getAbsolutePath(), PATTERN.pattern(),
                            Comparator.comparingLong(File::lastModified)
                    );
            List<Operation> uncommitedOperations = new ArrayList<>();
            for (File file : savedWriteAheadLogFiles) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        Operation operation = Operation.deserialize(line);
                        uncommitedOperations.add(operation);
                    }
                }
            }
            return uncommitedOperations;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removePreviousLogFiles(Long oldVersion) {
        File[] oldLogFiles =
                findFiles(storeDirectory.getAbsolutePath(), PATTERN.pattern(),
                        Comparator.comparingLong(File::lastModified)
                );
        for (File file : oldLogFiles) {
            Matcher matcher = PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                long fileVersion = Long.parseLong(matcher.group(1));
                if (fileVersion < oldVersion) {
                    file.delete();
                }
            }
        }
    }

    public static File[] findFiles(String dataDirectory, String pattern, Comparator<File> comparator) {
        File dir = new File(dataDirectory);

        File[] files = dir.listFiles((dir1, name) -> name.matches(pattern));

        Arrays.sort(files, comparator);
        return files;
    }

    private void adjustFileWriter() {
        try {
            writer = new FileWriter(writeAheadFile, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File createNewWal() {
        String filename = "_" + logicalTimeProvider.getVersion();
        return new File(storeDirectory.getAbsolutePath() + "/log" + filename + ".txt");
    }
}