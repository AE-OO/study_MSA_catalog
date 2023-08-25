package com.example.catalogsservice.service;

import com.example.catalogsservice.jpa.CatalogEntity;

public interface CatalogService {
    Iterable<CatalogEntity> getAllCatalogs();
}
