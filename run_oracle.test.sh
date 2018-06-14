export TZ=America/Los_Angeles
mvn -e -Djava.security.egd=file:/dev/./urandom -Ddatabase.url=jdbc:oracle:thin:@$ORACLEDB_SERVER:1521/ORCLCDB.localdomain -Ddatabase.user='sys as sysdba' -Ddatabase.password=Oradoc_db1 -P oracle12 verify
