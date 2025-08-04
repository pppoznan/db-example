package com.database.domain;

import com.database.replication.dto.response.ResponseManyKeysValuesDto;
import com.database.replication.dto.response.ResponseSingleKeyValueDto;

public interface ReadStorageProxy {

    ResponseSingleKeyValueDto read(Long key);
    ResponseManyKeysValuesDto readRange(Long from, Long to);
}
