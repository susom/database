# Database Library - Simplified JDBC Access

A Java library that provides a simplified wrapper around JDBC for safer, more reliable database access. Supports Oracle, PostgreSQL, SQL Server, HSQLDB, and Derby databases.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Prerequisites and Environment Setup
- **Java Version**: Requires Java 17 or later. Check with `java -version`.
- **Maven**: Maven 3.6+ is required. Check with `mvn -version`.
- **Docker**: Required for database-specific testing. Check with `docker --version`.
- **Timezone**: ALWAYS set timezone to America/Los_Angeles for consistent test results:
  ```bash
  export TZ=America/Los_Angeles
  ```

### Building and Testing
- **Basic Build**: 
  ```bash
  mvn clean compile
  ```
  - Takes 20-30 seconds
  - Compiles 49 Java source files
  - Warning about deprecated API in VertxUtil.java is expected
  
- **Full Build with Tests**:
  ```bash
  export TZ=America/Los_Angeles
  mvn clean package -Phsqldb \
    "-Duser.timezone=America/Los_Angeles" \
    "-Dhsqldb.database.url=jdbc:hsqldb:file:target/hsqldb;shutdown=true" \
    "-Dhsqldb.database.user=SA" \
    "-Dhsqldb.database.password="
  ```
  - **NEVER CANCEL: Takes 45-60 seconds. Set timeout to 90+ seconds.**
  - Runs 185 tests (some skipped)
  - Creates shaded JAR with HikariCP embedded
  - Generates Javadoc

- **Install to Local Repository**:
  ```bash
  mvn install -DskipTests
  ```
  - Takes 5-10 seconds
  - Required before building demo applications

### Database-Specific Testing with Docker

**CRITICAL**: All Docker-based tests take 3-10 minutes per database version. NEVER CANCEL these operations.

- **PostgreSQL Testing**:
  ```bash
  export TZ=America/Los_Angeles
  ./test-postgres.sh
  ```
  - **NEVER CANCEL: Takes 3-5 minutes per PostgreSQL version. Set timeout to 600+ seconds.**
  - Tests PostgreSQL versions 9.6, 10, 11, 12, 13, 14, 15, 16, 17
  - Each version downloads Docker image, starts container, runs tests, stops container

- **SQL Server Testing**:
  ```bash
  export TZ=America/Los_Angeles  
  ./test-sqlserver.sh
  ```
  - **NEVER CANCEL: Takes 2-3 minutes. Set timeout to 300+ seconds.**
  - Tests SQL Server 2019 only

- **Oracle Testing**:
  ```bash
  export TZ=America/Los_Angeles
  ./test-oracle.sh
  ```
  - **NEVER CANCEL: Takes 3-5 minutes. Set timeout to 600+ seconds.**
  - Tests Oracle 19.3
  - Requires access to internal artifact registry

### Direct Unit Testing (No Docker)
```bash
export TZ=America/Los_Angeles
mvn test -Phsqldb \
  "-Duser.timezone=America/Los_Angeles" \
  "-Dhsqldb.database.url=jdbc:hsqldb:file:target/hsqldb;shutdown=true" \
  "-Dhsqldb.database.user=SA" \
  "-Dhsqldb.database.password="
```
- **NEVER CANCEL: Takes 15-25 seconds. Set timeout to 60+ seconds.**
- Runs core tests using embedded HSQLDB and Derby
- Tests 185 cases with some expected skips

## Validation Scenarios

After making changes, ALWAYS validate functionality by running this complete test scenario:

1. **Clean Build Validation**:
   ```bash
   export TZ=America/Los_Angeles
   mvn clean compile test -Phsqldb \
     "-Duser.timezone=America/Los_Angeles" \
     "-Dhsqldb.database.url=jdbc:hsqldb:file:target/hsqldb;shutdown=true" \
     "-Dhsqldb.database.user=SA" \
     "-Dhsqldb.database.password="
   ```

2. **Library Installation**:
   ```bash
   mvn install -DskipTests
   ```

3. **Manual Functionality Test**: Run this Java validation code:
   ```bash
   # Create validation test (see example in /tmp/DatabaseValidationTest.java)
   javac -cp "target/database-5.0-SNAPSHOT.jar:/home/runner/.m2/repository/org/hsqldb/hsqldb/2.7.4/hsqldb-2.7.4.jar" YourValidationTest.java
   java -cp ".:target/database-5.0-SNAPSHOT.jar:/home/runner/.m2/repository/org/hsqldb/hsqldb/2.7.4/hsqldb-2.7.4.jar:/home/runner/.m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar:/home/runner/.m2/repository/ch/qos/reload4j/reload4j/1.2.19/reload4j-1.2.19.jar:/home/runner/.m2/repository/org/slf4j/slf4j-reload4j/1.7.36/slf4j-reload4j-1.7.36.jar" YourValidationTest
   ```

4. **SQL Injection Prevention Demo**: 
   ```bash
   cd demo
   mvn compile
   ```
   - **Expected to FAIL** - this demonstrates the Checker Framework detecting SQL injection vulnerabilities
   - Failure with 8 tainted string errors is the correct behavior

## CI/CD Integration

Before committing changes, ensure compatibility with CI pipeline:

- **GitHub Actions**: Check `.github/workflows/database.yaml`
- **SonarCloud Integration**: Code quality checks are automatic
- **Multi-Database Testing**: CI tests against PostgreSQL, SQL Server, Oracle, and HSQLDB
- **Build Artifact**: Deploys to Google Artifact Registry on master branch

## Common Tasks and File Locations

### Key Source Files
- **Main Library**: `src/main/java/com/github/susom/database/`
- **Core API**: `DatabaseProvider.java`, `Database.java`, `Schema.java`
- **Flavors**: Database-specific implementations in `src/main/java/com/github/susom/database/`
- **Tests**: `src/test/java/com/github/susom/database/test/`

### Configuration Files
- **Maven Build**: `pom.xml` (main project), `demo/pom.xml` (demo with Checker Framework)
- **Database Scripts**: `test-postgres.sh`, `test-sqlserver.sh`, `test-oracle.sh`
- **Sample Properties**: `sample.properties` (database connection examples)

### Example Usage Patterns
- **Simple Query**: `db.toSelect("select name from users where id=?").argLong(id).queryStringOrNull()`
- **Insert**: `db.toInsert("insert into table (col) values (?)").argString(value).insert(1)`
- **Transaction**: `DatabaseProvider.fromDriverManager(url).transact(db -> { /* operations */ })`
- **Schema Creation**: `new Schema().addTable("name").addColumn("col").asString(50).table().schema().execute(db)`

### Build Artifacts
- **Shaded JAR**: `target/database-5.0-SNAPSHOT.jar` (includes HikariCP)
- **Sources**: `target/database-5.0-SNAPSHOT-sources.jar`
- **Javadoc**: `target/database-5.0-SNAPSHOT-javadoc.jar`

## Troubleshooting

### Common Issues
- **Timezone Errors**: Always set `TZ=America/Los_Angeles` and include `-Duser.timezone=America/Los_Angeles`
- **Docker Container Timeouts**: Wait for health checks, containers can take 1-2 minutes to start
- **Missing Dependencies**: Use shaded JAR or include SLF4J, reload4j for standalone usage
- **SQL Injection Demo**: Intentionally fails compilation - this is correct behavior

### Performance Expectations
- **Basic compile**: 20-30 seconds
- **Full test suite**: 15-25 seconds 
- **Docker-based tests**: 3-10 minutes per database
- **CI pipeline**: 10-15 minutes total

Always validate that the library correctly handles database connections, transactions, prepared statements, and resource cleanup by running the complete test suite after any changes.