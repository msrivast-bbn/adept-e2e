package adept.e2e.documentdeduplication;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DbHashing {

  private Connection connection;
  private String db_sentence_name;
  private String db_overlap_name;


  public DbHashing(Map<String, String> param) throws SQLException {
    String db_server = param.get("db_server");
    String db_port = param.get("db_port");
    String db_name = param.get("db_name");
    db_sentence_name = param.get("db_sentence_table_name");
    db_overlap_name = param.get("db_overlap_table_name");
    String db_username = param.get("db_username");
    String db_password = param.get("db_password");

    String sqlUrl = "jdbc:postgresql://" + db_server + ":" + db_port + "/" + db_name;
    connection = DriverManager.getConnection(sqlUrl, db_username, db_password);
  }

  public boolean checkHash(byte[] hash) {
    PreparedStatement preparedStatement = null;
    ResultSet result = null;

    try {
      preparedStatement =
          connection.prepareStatement("SELECT * FROM \"" + db_sentence_name + "\" WHERE hash = ?");
      preparedStatement.setBytes(1, hash);
      result = preparedStatement.executeQuery();

      return result.next();
    } catch (Exception ex) {
      throw new RuntimeException("ERROR: Could not check hash against database.", ex);
    } finally {
      try {
        if (result != null) {
          result.close();
        }
      } catch (Exception e) {
      }
      ;
      try {
        if (preparedStatement != null) {
          preparedStatement.close();
        }
      } catch (Exception e) {
      }
      ;
    }
  }

  public void insertHashes(String docId, List<byte[]> hashes) {
    PreparedStatement hashInsertBatchStatement = null;

    try {
      hashInsertBatchStatement = connection.prepareStatement(
          "INSERT INTO \"" + db_sentence_name + "\" (documentname, hash) values (?,?)");
      for (int i = 0; i < hashes.size(); i++) {
        hashInsertBatchStatement.setString(1, docId);
        hashInsertBatchStatement.setBytes(2, hashes.get(i));
        hashInsertBatchStatement.addBatch();
      }
      hashInsertBatchStatement.executeBatch();
    } catch (Exception ex) {
      throw new RuntimeException("ERROR: Could not insert hashes into database.", ex);
    } finally {
      try {
        if (hashInsertBatchStatement != null) {
          hashInsertBatchStatement.close();
        }
      } catch (Exception e) {
      }
      ;
    }
  }

  public void insertOverlappingDoc(String docId, float score) {
    PreparedStatement preparedStatement = null;

    try {
      preparedStatement = connection.prepareStatement(
          "INSERT INTO \"" + db_overlap_name + "\" (documentname, score) values (?,?)");
      preparedStatement.setString(1, docId);
      preparedStatement.setFloat(2, score);
      preparedStatement.executeUpdate();
    } catch (Exception ex) {
      throw new RuntimeException("ERROR: Could not insert overlapping doc into database.", ex);
    } finally {
      try {
        if (preparedStatement != null) {
          preparedStatement.close();
        }
      } catch (Exception e) {
      }
      ;
    }
  }

  public void close() throws SQLException {
    connection.close();
  }

  private String hashToString(byte[] hash) throws NoSuchAlgorithmException {
    return new BigInteger(1, hash).toString(16);
  }
}
