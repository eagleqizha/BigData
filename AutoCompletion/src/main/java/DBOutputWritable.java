import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by qianzhang on 9/26/16.
 */
public class DBOutputWritable implements DBWritable{
    String starting_phrase;
    String following_phrase;
    int count;

    public DBOutputWritable(String starting_phrase, String following_phrase, int count) {
        this.starting_phrase = starting_phrase;
        this.following_phrase = following_phrase;
        this.count = count;
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, starting_phrase);
        preparedStatement.setString(2, following_phrase);
        preparedStatement.setInt(3, count);

    }

    public void readFields(ResultSet resultSet) throws SQLException {
        this.starting_phrase = resultSet.getString(1);
        this.following_phrase = resultSet.getString(2);
        this.count = resultSet.getInt(3);
    }
}
