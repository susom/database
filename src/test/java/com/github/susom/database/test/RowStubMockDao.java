package com.github.susom.database.test;

import com.github.susom.database.Database;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.function.Supplier;

/**
 * Test Create, read, update, and delete using the RowStub implementation.
 */
public class RowStubMockDao {
  private final Supplier<Database> dbp;

  public RowStubMockDao(Supplier<Database> dbp) {
    this.dbp = dbp;
  }

  public void create(final RowStubMockData data, Long userIdMakingChange) {
    Database db = dbp.get();

    Date updateTime = db.nowPerApp();
    Long dataId = db.toInsert(
      "insert into dbtest (data_id, name, local_date, update_sequence, update_time) values (?,?,?,0,?)")
      .argPkSeq("id_seq")
      .argString(data.getName())
      .argLocalDate(data.getLocalDate())
      .argDate(updateTime)
      .insertReturningPkSeq("data_id");

    // Update the object in memory
    data.setDataId(dataId);
    data.setUpdateSequence(0);
    data.setUpdateTime(updateTime);
  }

  public RowStubMockData findById(final Long dataId, ColumnLookupType lookupType, boolean lockRow) throws Exception {
    return dbp.get().toSelect("select name, local_date, update_sequence, update_time from dbtest where data_id=?"
      + (lockRow ? " for update" : ""))
      .argLong(dataId).queryOneOrNull(rowStub -> {
        RowStubMockData result = new RowStubMockData();
        result.setDataId(dataId);
        switch (lookupType) {
          case BY_ORDER:
            // Hit the column number path getting the results
            result.setName(rowStub.getStringOrNull());
            result.setLocalDate(rowStub.getLocalDateOrNull());
            result.setUpdateSequence(rowStub.getIntegerOrNull());
            result.setUpdateTime(rowStub.getDateOrNull());
            break;

          case BY_NAME:
            // Hig the column name path getting the results
            result.setName(rowStub.getStringOrNull("name"));
            result.setLocalDate(rowStub.getLocalDateOrNull("local_date"));
            result.setUpdateSequence(rowStub.getIntegerOrNull("update_sequence"));
            result.setUpdateTime(rowStub.getDateOrNull("update_time"));
            break;

          case BY_NUMBER:
            // Hit the column number path getting the results
            result.setName(rowStub.getStringOrNull(1));
            result.setLocalDate(rowStub.getLocalDateOrNull(2));
            result.setUpdateSequence(rowStub.getIntegerOrNull(3));
            result.setUpdateTime(rowStub.getDateOrNull(4));
            break;
          default:
            throw new Exception("Unexpected Lookup Type in findById!");
        }
        return result;
      });
  }

  public void update(RowStubMockData data, Long userIdMakingChange) {
    Database db = dbp.get();

    int newUpdateSequence = data.getUpdateSequence() + 1;
    Date newUpdateTime = db.nowPerApp();
    LocalDate newLocalDate = newUpdateTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

    db.toUpdate("update dbtest set name=?, local_date=?, update_sequence=?, update_time=? where data_id=?")
      .argString(data.getName())
      .argInteger(newUpdateSequence)
      .argLocalDate(newLocalDate)
      .argDate(newUpdateTime)
      .argLong(data.getDataId())
      .update(1);

    // Make sure the object in memory matches the database.
    data.setLocalDate(newLocalDate);
    data.setUpdateSequence(newUpdateSequence);
    data.setUpdateTime(newUpdateTime);
  }

  public void delete(RowStubMockData data, Long userIdMakingChange) {
    Database db = dbp.get();

    int newUpdateSequence = data.getUpdateSequence() + 1;
    Date newUpdateTime = db.nowPerApp();

    db.toDelete("delete from dbtest where data_id=?")
      .argLong(data.getDataId())
      .update(1);

    // Make sure the object in memory matches the database.
    data.setUpdateSequence(newUpdateSequence);
    data.setUpdateTime(newUpdateTime);
  }

  public enum ColumnLookupType {BY_ORDER, BY_NAME, BY_NUMBER}
}
