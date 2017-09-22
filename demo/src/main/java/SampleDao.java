import java.util.Date;
import java.util.function.Supplier;

import com.github.susom.database.Database;

/**
 * Create, read, update, and delete sample database objects.
 */
public class SampleDao {
  private final Supplier<Database> dbp;

  public SampleDao(Supplier<Database> dbp) {
    this.dbp = dbp;
  }

  public void createSample(final Sample sample, Long userIdMakingChange) {
    Database db = dbp.get();

    Date updateTime = db.nowPerApp();
    Long sampleId = db.toInsert(
        "insert into sample (sample_id, sample_name, update_sequence, update_time) values (?,?,0,?)")
        .argPkSeq("id_seq")
        .argString(sample.getName())
        .argDate(updateTime)
        .insertReturningPkSeq("sample_id");

    db.toInsert("insert into sample_history (sample_id, sample_name, update_sequence, update_time, update_user_id,"
        + " is_deleted) values (?,?,0,?,?,'N')")
        .argLong(sampleId)
        .argString(sample.getName())
        .argDate(updateTime)
        .argLong(userIdMakingChange)
        .insert(1);

    // Update the object in memory
    sample.setSampleId(sampleId);
    sample.setUpdateSequence(0);
    sample.setUpdateTime(updateTime);
  }

  public Sample findSampleById(final Long sampleId, boolean lockRow) {
    return dbp.get().toSelect("select sample_name, update_sequence, update_time from sample where sample_id=?"
        + (lockRow ? " for update" : ""))
        .argLong(sampleId).queryOneOrNull(r -> {
          Sample result = new Sample();
          result.setSampleId(sampleId);
          result.setName(r.getStringOrNull());
          result.setUpdateSequence(r.getIntegerOrNull());
          result.setUpdateTime(r.getDateOrNull());
          return result;
        });
  }

  public void updateSample(Sample sample, Long userIdMakingChange) {
    Database db = dbp.get();

    // Insert the history row first, so it will fail (non-unique sample_id + update_sequence)
    // if someone else modified the row. This is an optimistic locking strategy.
    int newUpdateSequence = sample.getUpdateSequence() + 1;
    Date newUpdateTime = db.nowPerApp();
    db.toInsert("insert into sample_history (sample_id, sample_name, update_sequence, update_time, update_user_id,"
        + " is_deleted) values (?,?,?,?,?,'N')")
        .argLong(sample.getSampleId())
        .argString(sample.getName())
        .argInteger(newUpdateSequence)
        .argDate(newUpdateTime)
        .argLong(userIdMakingChange)
        .insert(1);

    db.toUpdate("update sample set sample_name=?, update_sequence=?, update_time=? where sample_id=?")
        .argString(sample.getName())
        .argInteger(newUpdateSequence)
        .argDate(newUpdateTime)
        .argLong(sample.getSampleId())
        .update(1);

    // Make sure the object in memory matches the database.
    sample.setUpdateSequence(newUpdateSequence);
    sample.setUpdateTime(newUpdateTime);
  }

  public void deleteSample(Sample sample, Long userIdMakingChange) {
    Database db = dbp.get();

    // Insert the history row first, so it will fail (non-unique sample_id + update_sequence)
    // if someone else modified the row. This is an optimistic locking strategy.
    int newUpdateSequence = sample.getUpdateSequence() + 1;
    Date newUpdateTime = db.nowPerApp();
    db.toInsert("insert into sample_history (sample_id, sample_name, update_sequence, update_time, update_user_id,"
        + " is_deleted) values (?,?,?,?,?,'Y')")
        .argLong(sample.getSampleId())
        .argString(sample.getName())
        .argInteger(newUpdateSequence)
        .argDate(newUpdateTime)
        .argLong(userIdMakingChange)
        .insert(1);

    db.toDelete("delete from sample where sample_id=?")
        .argLong(sample.getSampleId())
        .update(1);

    // Make sure the object in memory matches the database.
    sample.setUpdateSequence(newUpdateSequence);
    sample.setUpdateTime(newUpdateTime);
  }
}
