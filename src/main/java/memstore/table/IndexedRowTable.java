package memstore.table;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        index = new TreeMap<>();
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int field = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, field);
                if(colId == indexColumn) {
                    IntArrayList intArrayList = index.getOrDefault(field, new IntArrayList());
                    intArrayList.add(rowId);
                    index.put(field, intArrayList);
                }
            }
        }

    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset =  ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);

        if (colId == indexColumn) {
            int value = rows.getInt(offset);
            index.get(value).rem(rowId);
            if (index.get(value).isEmpty()) index.remove(value);
            index.putIfAbsent(field, new IntArrayList());
            index.get(field).add(rowId);
        }
        rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        long sum = 0;
        if (indexColumn == 0) {
            for (int value : index.keySet()) {
                sum += value * index.get(value).size();
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                sum += getIntField(rowId, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!

        long sum = 0;
        if (indexColumn == 1) {
            for (IntArrayList iter : index.tailMap(threshold1 + 1).values()) {
                for (int rowId : iter) {
                    if (getIntField(rowId, 2) < threshold2) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
        } else if (indexColumn == 2) {
            for (IntArrayList iter : index.headMap(threshold2).values()) {
                for (int rowId : iter) {
                    if (getIntField(rowId, 1) > threshold1) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 1) <= threshold1 || getIntField(rowId, 2) >= threshold2) {
                    continue;
                }
                sum += getIntField(rowId, 0);
            }
        }
        return sum;
    }


    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        long sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            if(getIntField(rowId, 0) > threshold) {
                for (int colId = 0; colId < numCols; colId++) {
                    sum += getIntField(rowId, colId);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        int updatedRows = 0;
        if (indexColumn == 0) {
            for (IntArrayList iter : index.headMap(threshold).values()) {
                updatedRows += iter.size();
                for (int rowId : iter) {
                    putIntField(rowId, 3, getIntField(rowId, 2) + getIntField(rowId, 3));
                }
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 0) >= threshold) {
                    continue;
                }
                updatedRows++;
                putIntField(rowId, 3, getIntField(rowId, 2) + getIntField(rowId, 3));
            }
        }
        return updatedRows;
    }
}
