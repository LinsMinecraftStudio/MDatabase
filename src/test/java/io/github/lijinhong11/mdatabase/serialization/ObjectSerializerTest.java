package io.github.lijinhong11.mdatabase.serialization;

import io.github.lijinhong11.mdatabase.serialization.annotations.Column;
import org.junit.jupiter.api.Test;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ObjectSerializerTest {
    private static void printBytes(String label, byte[] bytes) {
        System.out.println(label + " bytes=" + bytes.length);
    }

    private static void printValue(String label, Object value) {
        System.out.println(label + "=" + value);
    }

    @Test
    void convertInterfaceAndConcreteCollectionsBackToBlob() {
        ArrayList<String> list = new ArrayList<>(List.of("a", "b"));
        LinkedHashSet<Integer> set = new LinkedHashSet<>(List.of(1, 2));
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("x", 1);
        map.put("y", 2);

        byte[] listBytes = (byte[]) ObjectSerializer.convertBack(list);
        byte[] setBytes = (byte[]) ObjectSerializer.convertBack(set);
        byte[] mapBytes = (byte[]) ObjectSerializer.convertBack(map);

        System.out.println("Printing test data: ");
        
        printValue("list", list);
        printBytes("list", listBytes);
        printValue("set", set);
        printBytes("set", setBytes);
        printValue("map", map);
        printBytes("map", mapBytes);

        assertInstanceOf(byte[].class, listBytes);
        assertInstanceOf(byte[].class, setBytes);
        assertInstanceOf(byte[].class, mapBytes);

        assertEquals("BLOB", ObjectSerializer.getSqlType(List.class));
        assertEquals("BLOB", ObjectSerializer.getSqlType(Set.class));
        assertEquals("BLOB", ObjectSerializer.getSqlType(Map.class));
    }

    @Test
    void convertAtomicTypesToNativeSqlValues() {
        Object atomicInteger = ObjectSerializer.convertBack(new AtomicInteger(7));
        Object atomicLong = ObjectSerializer.convertBack(new AtomicLong(9L));
        Object atomicBoolean = ObjectSerializer.convertBack(new AtomicBoolean(true));

        printValue("atomicInteger", atomicInteger);
        printValue("atomicLong", atomicLong);
        printValue("atomicBoolean", atomicBoolean);

        assertEquals(7, atomicInteger);
        assertEquals(9L, atomicLong);
        assertEquals(true, atomicBoolean);

        assertEquals("INTEGER", ObjectSerializer.getSqlType(AtomicInteger.class));
        assertEquals("BIGINT", ObjectSerializer.getSqlType(AtomicLong.class));
        assertEquals("BOOLEAN", ObjectSerializer.getSqlType(AtomicBoolean.class));
    }

    @Test
    void serializeCollectionsAndAtomicFieldsFromResultSet() throws Exception {
        byte[] tagsBytes = (byte[]) ObjectSerializer.convertBack(new ArrayList<>(List.of("red", "blue")));
        byte[] scoresBytes = (byte[]) ObjectSerializer.convertBack(new LinkedHashSet<>(List.of(1, 3, 5)));
        LinkedHashMap<String, Integer> metadata = new LinkedHashMap<>();
        metadata.put("alpha", 10);
        metadata.put("beta", 20);
        byte[] metadataBytes = (byte[]) ObjectSerializer.convertBack(metadata);
        printValue("tags source", List.of("red", "blue"));
        printBytes("tags", tagsBytes);
        printValue("scores source", new LinkedHashSet<>(List.of(1, 3, 5)));
        printBytes("scores", scoresBytes);
        printValue("metadata source", metadata);
        printBytes("metadata", metadataBytes);

        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(6);
        metaData.setColumnName(1, "tags");
        metaData.setColumnType(1, Types.LONGVARBINARY);
        metaData.setColumnName(2, "scores");
        metaData.setColumnType(2, Types.LONGVARBINARY);
        metaData.setColumnName(3, "metadata");
        metaData.setColumnType(3, Types.LONGVARBINARY);
        metaData.setColumnName(4, "counter");
        metaData.setColumnType(4, Types.INTEGER);
        metaData.setColumnName(5, "big_counter");
        metaData.setColumnType(5, Types.BIGINT);
        metaData.setColumnName(6, "enabled");
        metaData.setColumnType(6, Types.BOOLEAN);
        rowSet.setMetaData(metaData);
        rowSet.moveToInsertRow();
        rowSet.updateBytes(1, tagsBytes);
        rowSet.updateBytes(2, scoresBytes);
        rowSet.updateBytes(3, metadataBytes);
        rowSet.updateInt(4, 11);
        rowSet.updateLong(5, 15L);
        rowSet.updateBoolean(6, true);
        rowSet.insertRow();
        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        rowSet.next();

        ComplexEntity entity = ObjectSerializer.serializeOne(ComplexEntity.class, rowSet);
        printValue("entity.tags", entity.tags);
        printValue("entity.scores", entity.scores);
        printValue("entity.metadata", entity.metadata);
        printValue("entity.counter", entity.counter.get());
        printValue("entity.bigCounter", entity.bigCounter.get());
        printValue("entity.enabled", entity.enabled.get());

        assertEquals(List.of("red", "blue"), entity.tags);
        assertEquals(new LinkedHashSet<>(List.of(1, 3, 5)), entity.scores);
        assertEquals(metadata, entity.metadata);
        assertEquals(11, entity.counter.get());
        assertEquals(15L, entity.bigCounter.get());
        assertTrue(entity.enabled.get());
    }

    @Test
    void serializeConcreteCollectionFieldsFromResultSet() throws Exception {
        byte[] valuesBytes = (byte[]) ObjectSerializer.convertBack(new ArrayList<>(List.of("left", "right")));
        printValue("values source", List.of("left", "right"));
        printBytes("values", valuesBytes);

        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(1);
        metaData.setColumnName(1, "values");
        metaData.setColumnType(1, Types.LONGVARBINARY);
        rowSet.setMetaData(metaData);
        rowSet.moveToInsertRow();
        rowSet.updateBytes(1, valuesBytes);
        rowSet.insertRow();
        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        rowSet.next();

        ConcreteCollectionEntity entity = ObjectSerializer.serializeOne(ConcreteCollectionEntity.class, rowSet);
        printValue("entity.values", entity.values);

        assertEquals(List.of("left", "right"), entity.values);
        assertArrayEquals(valuesBytes, (byte[]) ObjectSerializer.convertBack(entity.values));
    }

    @Test
    void restoreNestedGenericAtomicValues() throws Exception {
        byte[] countersBytes = (byte[]) ObjectSerializer.convertBack(
                new ArrayList<>(List.of(new AtomicInteger(2), new AtomicInteger(4))),
                NestedGenericEntity.class.getDeclaredField("counters").getGenericType()
        );
        LinkedHashMap<String, AtomicLong> sequences = new LinkedHashMap<>();
        sequences.put("a", new AtomicLong(6L));
        sequences.put("b", new AtomicLong(8L));
        byte[] sequencesBytes = (byte[]) ObjectSerializer.convertBack(
                sequences,
                NestedGenericEntity.class.getDeclaredField("sequences").getGenericType()
        );
        printValue("counters source", List.of(new AtomicInteger(2), new AtomicInteger(4)));
        printBytes("counters", countersBytes);
        printValue("sequences source", sequences);
        printBytes("sequences", sequencesBytes);

        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(2);
        metaData.setColumnName(1, "counters");
        metaData.setColumnType(1, Types.LONGVARBINARY);
        metaData.setColumnName(2, "sequences");
        metaData.setColumnType(2, Types.LONGVARBINARY);
        rowSet.setMetaData(metaData);
        rowSet.moveToInsertRow();
        rowSet.updateBytes(1, countersBytes);
        rowSet.updateBytes(2, sequencesBytes);
        rowSet.insertRow();
        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        rowSet.next();

        NestedGenericEntity entity = ObjectSerializer.serializeOne(NestedGenericEntity.class, rowSet);
        printValue("entity.counters", entity.counters);
        printValue("entity.sequences", entity.sequences);

        assertEquals(2, entity.counters.get(0).get());
        assertEquals(4, entity.counters.get(1).get());
        assertEquals(6L, entity.sequences.get("a").get());
        assertEquals(8L, entity.sequences.get("b").get());
    }

    @Test
    void restoreCustomEntityValues() throws Exception {
        LinkedHashSet<ACustomObject> objects = new LinkedHashSet<>();
        objects.add(createCustomObject("cat", true));
        objects.add(createCustomObject("stone", false));
        ACustomObject anotherOne = createCustomObject("tree", true);

        byte[] objectsBytes = (byte[]) ObjectSerializer.convertBack(
                objects,
                CustomEntity.class.getDeclaredField("objects").getGenericType()
        );
        byte[] anotherOneBytes = (byte[]) ObjectSerializer.convertBack(
                anotherOne,
                CustomEntity.class.getDeclaredField("anotherOne").getGenericType()
        );
        printValue("custom objects source", objects);
        printBytes("custom objects", objectsBytes);
        printValue("anotherOne source", anotherOne);
        printBytes("anotherOne", anotherOneBytes);

        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(2);
        metaData.setColumnName(1, "objects");
        metaData.setColumnType(1, Types.LONGVARBINARY);
        metaData.setColumnName(2, "anotherOne");
        metaData.setColumnType(2, Types.LONGVARBINARY);
        rowSet.setMetaData(metaData);
        rowSet.moveToInsertRow();
        rowSet.updateBytes(1, objectsBytes);
        rowSet.updateBytes(2, anotherOneBytes);
        rowSet.insertRow();
        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        rowSet.next();

        CustomEntity entity = ObjectSerializer.serializeOne(CustomEntity.class, rowSet);
        printValue("entity.objects", entity.objects);
        printValue("entity.anotherOne", entity.anotherOne);

        List<ACustomObject> restoredObjects = new ArrayList<>(entity.objects);
        assertEquals(2, restoredObjects.size());
        assertEquals("cat", restoredObjects.get(0).whatIsThat);
        assertTrue(restoredObjects.get(0).living);
        assertEquals("stone", restoredObjects.get(1).whatIsThat);
        assertFalse(restoredObjects.get(1).living);
        assertEquals("tree", entity.anotherOne.whatIsThat);
        assertTrue(entity.anotherOne.living);
    }

    private static ACustomObject createCustomObject(String whatIsThat, boolean living) {
        ACustomObject object = new ACustomObject();
        object.whatIsThat = whatIsThat;
        object.living = living;
        return object;
    }

    static class ComplexEntity {
        @Column
        public List<String> tags;
        @Column
        public Set<Integer> scores;
        @Column
        public Map<String, Integer> metadata;
        @Column
        public AtomicInteger counter;
        @Column(name = "big_counter")
        public AtomicLong bigCounter;
        @Column
        public AtomicBoolean enabled;
    }

    static class ConcreteCollectionEntity {
        @Column
        public ArrayList<String> values;
    }

    static class NestedGenericEntity {
        @Column
        public List<AtomicInteger> counters;
        @Column
        public Map<String, AtomicLong> sequences;
    }

    static class CustomEntity {
        @Column
        public Set<ACustomObject> objects;

        @Column
        public ACustomObject anotherOne;
    }

    static class ACustomObject {
        @Column
        public String whatIsThat;

        @Column
        public boolean living;

        @Override
        public String toString() {
            return "ACustomObject{" +
                    "whatIsThat='" + whatIsThat + '\'' +
                    ", living=" + living +
                    '}';
        }
    }
}
