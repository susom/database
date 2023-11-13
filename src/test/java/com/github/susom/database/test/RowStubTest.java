package com.github.susom.database.test;

import com.github.susom.database.*;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;
import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for exercising the RowStub implementation
 */
public class RowStubTest {
  @Mock
  private DatabaseMock db;
  private Date now = new Date();
  private LocalDate localDateNow = LocalDate.now();
  private RowStubMockDao rowStubMockDao;

  @Before
  public void initializeMocks() {
    MockitoAnnotations.initMocks(this);

    // This is the key for database mocking. Explicitly create our DatabaseImpl and give it our mock.
    // Could use any Flavor here, though postgres/oracle are a little easier than derby because
    // they create single operations for insert returning.
    rowStubMockDao = new RowStubMockDao(new DatabaseImpl(db, new OptionsOverride(Flavor.postgresql) {
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

    RowStubMockData data = new RowStubMockData();
    data.setName("Foo");
    data.setLocalDate(localDateNow);

    rowStubMockDao.create(data, 1L);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(1L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(localDateNow, data.getLocalDate());
    assertEquals(Integer.valueOf(0), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify SQL executed against golden copies
    verify(db).insertReturningPk(eq("insert into dbtest (data_id, name, local_date, update_sequence, update_time) values (nextval('id_seq'),?,?,0,?)"), anyString());
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testFindByColumnOrder() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
      .withColumnNames("name", "local_date", "update_sequence", "update_time")
      .addRow("Foo", localDateNow, 3, now));

    // The test scenario
    RowStubMockData data = rowStubMockDao.findById(12L, RowStubMockDao.ColumnLookupType.BY_ORDER, false);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(12L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(localDateNow, data.getLocalDate());
    assertEquals(Integer.valueOf(3), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).query(anyString(), eq("select name, local_date, update_sequence, update_time from dbtest where data_id=12"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testFindByColumnNames() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
      .withColumnNames("name", "local_date", "update_sequence", "update_time")
      .addRow("Foo", localDateNow, 3, now));

    // The test scenario
    RowStubMockData data = rowStubMockDao.findById(13L, RowStubMockDao.ColumnLookupType.BY_NAME, false);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(13L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(localDateNow, data.getLocalDate());
    assertEquals(Integer.valueOf(3), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).query(anyString(), eq("select name, local_date, update_sequence, update_time from dbtest where data_id=13"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testFindAndLock() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
      .withColumnNames("name", "local_date", "update_sequence", "update_time")
      .addRow("Foo", localDateNow, 3, now));

    // The test scenario
    RowStubMockData data = rowStubMockDao.findById(15L, RowStubMockDao.ColumnLookupType.BY_NUMBER, true);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(15L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(localDateNow, data.getLocalDate());
    assertEquals(Integer.valueOf(3), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).query(anyString(), eq("select name, local_date, update_sequence, update_time from dbtest where data_id=15 for update"));
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testUpdate() throws Exception {
    // Configure the mock because our class under test expects values to be returned from the db
    when(db.query(anyString(), anyString())).thenReturn(new RowStub()
      .withColumnNames("name", "local_date", "update_sequence", "update_time")
      .addRow("Foo", localDateNow, 3, now));
    Date before = new Date(now.getTime() - 5000);

    RowStubMockData data = new RowStubMockData();
    data.setDataId(100L);
    data.setName("Foo");
    data.setLocalDate(localDateNow);
    data.setUpdateSequence(13);
    data.setUpdateTime(before);
    rowStubMockDao.update(data, 23L);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(100L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(localDateNow, data.getLocalDate());
    assertEquals(Integer.valueOf(14), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).update(eq("update dbtest set name=?, local_date=?, update_sequence=?, update_time=? where data_id=?"), anyString());
    verifyNoMoreInteractions(db);
  }

  @Test
  public void testDelete() throws Exception {
    Date before = new Date(now.getTime() - 5000);

    RowStubMockData data = new RowStubMockData();
    data.setDataId(100L);
    data.setName("Foo");
    data.setUpdateSequence(13);
    data.setUpdateTime(before);
    rowStubMockDao.delete(data, 23L);

    // Verify object in memory is updated properly
    assertEquals(Long.valueOf(100L), data.getDataId());
    assertEquals("Foo", data.getName());
    assertEquals(Integer.valueOf(14), data.getUpdateSequence());
    assertEquals(now, data.getUpdateTime());

    // Verify database queries against golden copies
    verify(db).update(anyString(), eq("delete from dbtest where data_id=100"));
    verifyNoMoreInteractions(db);
  }
}
