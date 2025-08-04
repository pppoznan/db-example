package com.database.domain;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class DigestUtil {
    private final MessageDigest messageDigest;

    public DigestUtil(MessageDigest messageDigest) {
        this.messageDigest = messageDigest;
    }

    String getHashAsString(String value) {
        byte[] inputBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] hashBytes = messageDigest.digest(inputBytes);

        BigInteger hashInteger = new BigInteger(1, hashBytes);

        StringBuilder hexString = new StringBuilder(hashInteger.toString(16));
        while (hexString.length() < 64) {
            hexString.insert(0, "0");
        }

        return hexString.toString();
    }
}