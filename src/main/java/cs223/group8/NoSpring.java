package cs223.group8;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cs223.group8.entity.DataItem;
import cs223.group8.mapper.DataItemMapper;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;


public class NoSpring {

    private static SqlSessionFactory sqlSessionFactory = initSqlSessionFactory();

    public static void main(String[] args) {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            DataItemMapper mapper = session.getMapper(DataItemMapper.class);
            QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("key", "z");
            DataItem item = mapper.selectOne(queryWrapper);
            System.out.println(item.getValue());
        }
    }

    public static SqlSessionFactory initSqlSessionFactory() {
        DataSource dataSource = dataSource();
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("Production", transactionFactory, dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration(environment);
        configuration.addMapper(DataItemMapper.class);
        configuration.setLogImpl(StdOutImpl.class);
        return new MybatisSqlSessionFactoryBuilder().build(configuration);
    }

    public static DataSource dataSource() {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(org.postgresql.Driver.class);
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:5432/leader");
        dataSource.setUsername("postgres");
        dataSource.setPassword("lyq990515");
//        try {
//            Connection connection = dataSource.getConnection();
//            Statement statement = connection.createStatement();
//            statement.execute("create table person (" +
//                    "id BIGINT(20) NOT NULL," +
//                    "name VARCHAR(30) NULL," +
//                    "age INT(11) NULL," +
//                    "PRIMARY KEY (id)" +
//                    ")");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        return dataSource;
    }
}