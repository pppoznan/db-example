package com.database.domain;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PersistentStorage {
    private static final Logger LOGGER = Logger.getLogger(PersistentStorage.class.getName());

    public static final int MAX_CHARS_TO_COMPUTE_INDEX = 1;
    private final File storeDirectory;
    private final DigestUtil digestUtil;

    public PersistentStorage(String directory) {
        storeDirectory = new File(directory + "/STORE");
        if (!storeDirectory.exists() || !storeDirectory.isDirectory())
            storeDirectory.mkdirs();
        try {
            this.digestUtil = new DigestUtil(MessageDigest.getInstance("SHA-256"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(Set<Entry<Long, KeyValueEntry>> entriesToSaveOrUpdate) {
        Map<String, List<Entry<Long, KeyValueEntry>>> entriesByDifferentiator = entriesToSaveOrUpdate.stream()
                .collect(Collectors.groupingBy(this::getFileDifferentiator));

        for (Entry<String, List<Entry<Long, KeyValueEntry>>> entry : entriesByDifferentiator.entrySet()) {
            try {
                String fileDifferentiator = entry.getKey();

                File file = getFile(storeDirectory, fileDifferentiator);
                File tmpFile = getTmpFile(storeDirectory, fileDifferentiator);
                List<KeyValueEntry> exisingData = file.exists()
                        ? Index.readFromFile(file)
                        : Collections.emptyList();

                List<KeyValueEntry> newData = entry.getValue().stream()
                        .map(Entry::getValue)
                        .toList();

                List<KeyValueEntry> mergedData =
                        ListMerger.mergeListsPrioritizingNewData(exisingData, newData)
                                .stream()
                                .filter(e -> !Objects.equals(e.value(), KeyValueEntry.EMPTY_VALUE))
                                .sorted()
                                .toList();

                writeFile(tmpFile, mergedData);
                tmpFile.renameTo(file);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public Optional<KeyValueEntry> read(Long key) {
        List<KeyValueEntry> entries = readMany(List.of(key));
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(entries.get(0));
    }

    public List<KeyValueEntry> readMany(List<Long> keys) {
        Map<String, List<Long>> keysSplittedToFiles = keys.stream()
                .collect(Collectors.<Long, String>groupingBy(this::getFileDifferentiator));

        List<KeyValueEntry> entries = new ArrayList<>();

        for (Map.Entry<String, List<Long>> entry : keysSplittedToFiles.entrySet()) {
            File file = getFile(storeDirectory, entry.getKey());
            if (!file.exists()) {
                continue;
            }
            Long fromKey = entry.getValue().getFirst();
            Long toKey = entry.getValue().getLast();
            entries.addAll(Index.readFromFile(file, fromKey, toKey));
        }
        return entries;
    }

    private String getFileDifferentiator(Entry<Long, KeyValueEntry> entry) {
        return getFileDifferentiator(entry.getKey());
    }

    private static File getFile(File storeDirectory, String fileDifferentiator) {
        return new File(storeDirectory.getAbsolutePath() + "/" + getFileName(fileDifferentiator));
    }

    private static File getTmpFile(File storeDirectory, String fileDifferentiator) {
        return new File(storeDirectory.getAbsolutePath() + "/" + getTmpFileName(fileDifferentiator));
    }

    private String getFileDifferentiator(Long key) {
        String keyValue = String.valueOf(key);
        if (keyValue.length() > MAX_CHARS_TO_COMPUTE_INDEX) {
            return digestUtil.getHashAsString(keyValue.substring(0, MAX_CHARS_TO_COMPUTE_INDEX));
        }
        return digestUtil.getHashAsString(keyValue);
    }

    private static String getFileName(String fileDifferentiator) {
        return "data_" + fileDifferentiator + ".bin";
    }

    private static String getTmpFileName(String fileDifferentiator) {
        return "data_" + fileDifferentiator + "_tmp.bin";
    }

    private static void writeFile(File file, List<KeyValueEntry> entries) {
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try (FileOutputStream fos = new FileOutputStream(file);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                FileChannel channel = fos.getChannel()) {
            Index index = new Index(entries);
            index.saveIndex(channel);
            for (KeyValueEntry entry : entries) {
                bos.write(entry.value().getBytes(StandardCharsets.UTF_8));
            }
            bos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record Index(List<KeyValueEntry> entries) {

        public void saveIndex(FileChannel channel) {
            try {
                channel.write(ByteBuffer.allocate(4).putInt(entries.size()).flip());
                int startPositionOffset = entries.size() * 16 + 4;
                int endPositionOffset = startPositionOffset;

                for (KeyValueEntry entry : entries) {
                    endPositionOffset += entry.value().getBytes(StandardCharsets.UTF_8).length;

                    channel.write(ByteBuffer.allocate(8).putLong(entry.key()).flip());
                    channel.write(ByteBuffer.allocate(4).putInt(startPositionOffset).flip());
                    channel.write(ByteBuffer.allocate(4).putInt(endPositionOffset).flip());

                    startPositionOffset += entry.value().getBytes(StandardCharsets.UTF_8).length;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static List<KeyValueEntry> readFromFile(File file) {
            return readFromFile(file, 0L, Long.MAX_VALUE);
        }

        public static List<KeyValueEntry> readFromFile(File file, Long fromKey, Long toKey) {
            List<KeyValueEntry> entries = new ArrayList<>();
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(0);
                int numOfEntries = raf.readInt();

                List<IndexEntry> indexEntries = new ArrayList<>();
                for (int i = 0; i < numOfEntries; i++) {
                    long keyBytes = raf.readLong();
                    int dataStartOffset = raf.readInt();
                    int dataEndOffset = raf.readInt();
                    indexEntries.add(new IndexEntry(keyBytes, dataStartOffset, dataEndOffset));
                }
                int fromIndex = Collections.binarySearch(indexEntries, new IndexEntry(fromKey, null, null),
                        Comparator.comparing(IndexEntry::key)
                );
                int toIndex = Collections.binarySearch(indexEntries, new IndexEntry(toKey, null, null),
                        Comparator.comparing(IndexEntry::key)
                );
                if (fromIndex < 0 && fromKey != 0) {
                    return Collections.emptyList();
                }
                if (fromKey == 0) {
                    fromIndex = 0;
                }
                if (toIndex < 0) {
                    toIndex = indexEntries.size() - 1;
                }
                List<IndexEntry> entriesToFetches = indexEntries.subList(fromIndex, toIndex + 1);

                for (IndexEntry entryToFetch : entriesToFetches) {
                    int dataLength = entryToFetch.end() - entryToFetch.start();
                    byte[] bytes = new byte[dataLength];
                    raf.seek(entryToFetch.start());
                    raf.read(bytes, 0, dataLength);
                    String data = new String(bytes);
                    entries.add(new KeyValueEntry(Long.valueOf(entryToFetch.key()), data));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return entries;
        }

    }

    record IndexEntry(long key, Integer start, Integer end) {}

}
