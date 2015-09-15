import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.susom.database.DatabaseImpl;
import com.github.susom.database.DatabaseMock;
import com.github.susom.database.Flavor;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.RowStub;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the API for creating new visit IDs.
 */
public class SampleDaoTest {
  @Mock
  DatabaseMock db;
  Date now = new Date();
  SampleDao sampleDao;

  @Before
  public void initializeMocks() {
    MockitoAnnotations.initMocks(this);

    // This is the key for database mocking. Explicitly create our DatabaseImpl and give it our mock.
    // Could use any Flavor here, though postgres/oracle are a little easier than derby because
    // they create single operations for insert returning.
    sampleDao = new SampleDao(new DatabaseImpl(db, new OptionsOverride(Flavor.postgresql) {
      @Override
      public Date currentDate() {
        // Use a local variable from the test so we can verify in a deterministic way
        return now;
      }
    }));
  }

  @Test
  public void testCreate() throws Exception {
    // Configure the mock because DAO expects the pk to be returned from the insert
    when(db.insertReturningPk(anyString(), anyString())).thenReturn(1L);

    Sample sample = new Sample();
    sample.setName("Foo");

    sampleDao.createSample(sample, 1L);

    // Verify object in memory is updated properly
    assertEquals(new Long(1L), sample.getSampleId());
    assertEquals("Foo", sample.getName());
    assertEquals(new Integer(0), sample.getUpdateSequence());
    assertEquals(now, sample.getUpdateTime());

    // Verify SQL executed against golden copies
    verify(db).insertReturningPk(anyString(), matches("insert into sample \\(sample_id, sample_name, update_sequence, update_time\\) values \\(nextval\\('id_seq'\\),'Foo',0,to_timestamp\\('.{26}', 'YYYY-MM-DD HH24:MI:SS.US'\\)\\)"));
    verify(db).insert(anyString(), matches("insert into sample_history \\(sample_id, sample_name, update_sequence, update_time, update_user_id, is_deleted\\) values \\(1,'Foo',0,to_timestamp\\('.{26}', 'YYYY-MM-DD HH24:MI:SS.US'\\),1,'N'\\)"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testFind() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
        .withColumnNames("sample_name", "update_sequence", "update_time")
        .addRow("Foo", 3, now));

    // The test scenario
    Sample sample = sampleDao.findSampleById(15L, false);

    // Verify object in memory is updated properly
    assertEquals(new Long(15L), sample.getSampleId());
    assertEquals("Foo", sample.getName());
    assertEquals(new Integer(3), sample.getUpdateSequence());
    assertEquals(now, sample.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).query(anyString(), eq("select sample_name, update_sequence, update_time from sample where sample_id=15"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testFindAndLock() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
        .withColumnNames("sample_name", "update_sequence", "update_time")
        .addRow("Foo", 3, now));

    // The test scenario
    Sample sample = sampleDao.findSampleById(15L, true);

    // Verify object in memory is updated properly
    assertEquals(new Long(15L), sample.getSampleId());
    assertEquals("Foo", sample.getName());
    assertEquals(new Integer(3), sample.getUpdateSequence());
    assertEquals(now, sample.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).query(anyString(), eq("select sample_name, update_sequence, update_time from sample where sample_id=15 for update"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testUpdate() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
        .withColumnNames("sample_name", "update_sequence", "update_time")
        .addRow("Foo", 3, now));
    Date before = new Date(now.getTime() - 5000);

    Sample sample = new Sample();
    sample.setSampleId(100L);
    sample.setName("Foo");
    sample.setUpdateSequence(13);
    sample.setUpdateTime(before);
    sampleDao.updateSample(sample, 23L);

    // Verify object in memory is updated properly
    assertEquals(new Long(100L), sample.getSampleId());
    assertEquals("Foo", sample.getName());
    assertEquals(new Integer(14), sample.getUpdateSequence());
    assertEquals(now, sample.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).update(anyString(), matches("update sample set sample_name='Foo', update_sequence=14, update_time=to_timestamp\\('.{26}', 'YYYY-MM-DD HH24:MI:SS.US'\\) where sample_id=100"));
    verify(db).insert(anyString(), matches("insert into sample_history \\(sample_id, sample_name, update_sequence, update_time, update_user_id, is_deleted\\) values \\(100,'Foo',14,to_timestamp\\('.{26}', 'YYYY-MM-DD HH24:MI:SS.US'\\),23,'N'\\)"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testDelete() throws Exception {
    Date before = new Date(now.getTime() - 5000);

    Sample sample = new Sample();
    sample.setSampleId(100L);
    sample.setName("Foo");
    sample.setUpdateSequence(13);
    sample.setUpdateTime(before);
    sampleDao.deleteSample(sample, 23L);

    // Verify object in memory is updated properly
    assertEquals(new Long(100L), sample.getSampleId());
    assertEquals("Foo", sample.getName());
    assertEquals(new Integer(14), sample.getUpdateSequence());
    assertEquals(now, sample.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).update(anyString(), eq("delete from sample where sample_id=100"));
    verify(db).insert(anyString(), matches("insert into sample_history \\(sample_id, sample_name, update_sequence, update_time, update_user_id, is_deleted\\) values \\(100,'Foo',14,to_timestamp\\('.{26}', 'YYYY-MM-DD HH24:MI:SS.US'\\),23,'Y'\\)"));
    verifyNoMoreInteractions(db);
  }
}
