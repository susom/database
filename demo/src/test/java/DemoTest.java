import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Make sure each demo class runs and outputs the correct values.
 */
public class DemoTest {
  @Test
  public void dynamicSql() {
    final Output output = mock(Output.class);

    new DynamicSql() {
      @Override
      public void println(String s) {
        output.println(s);
      }
    }.launch(new String[0]);

    verify(output).println("Rows with none: 2");
    verify(output).println("Rows with pk=1: 1");
    verify(output).println("Rows with s=Hi: 2");
    verifyNoMoreInteractions(output);
  }

  @Test
  public void helloDerby() {
    final Output output = mock(Output.class);

    new HelloDerby() {
      @Override
      public void println(String s) {
        output.println(s);
      }
    }.run();

    verify(output).println("Rows: 1");
    verifyNoMoreInteractions(output);
  }

  @Test
  public void fakeBuilder() {
    final Output output = mock(Output.class);

    new FakeBuilder() {
      @Override
      public void println(String s) {
        output.println(s);
      }
    }.launch(new String[0]);

    verify(output).println("Rows before rollback: 2");
    verify(output).println("Correctly threw exception: Called get() on a DatabaseProvider after close()");
    verify(output).println("Rows after rollback: 0");
    verifyNoMoreInteractions(output);
  }

  @Test
  public void insertReturning() {
    final Output output = mock(Output.class);

    new InsertReturning() {
      @Override
      public void println(String s) {
        output.println(s);
      }
    }.launch(new String[0]);

    verify(output).println("Inserted row with pk=1");
    verifyNoMoreInteractions(output);
  }

  interface Output {
    void println(String s);
  }
}
