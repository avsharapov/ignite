package ru.inno.stc.services;

public interface SimpleMapService<K,V> {
    void put(K key, V value);
    int size();
}
