package cs223.group8.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cs223.group8.entity.DataItem;
import cs223.group8.mapper.DataItemMapper;
import cs223.group8.session.GeneralSessionConfig;
import cs223.group8.utils.LogParser;
import org.apache.ibatis.session.SqlSession;

public class GeneralDatasourceRepository {

    public Integer readItemValue(String key) {
        SqlSession session = GeneralSessionConfig.sqlSession;
        DataItemMapper mapper = session.getMapper(DataItemMapper.class);
        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("key", key);
        DataItem item = mapper.selectOne(queryWrapper);
        return item.getValue();
    }


    public void writeItem(String key, Integer value) {
        SqlSession session = GeneralSessionConfig.sqlSession;
        DataItemMapper mapper = session.getMapper(DataItemMapper.class);
        DataItem item = new DataItem(key, value);
        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("key", key);
        if(null == mapper.selectOne(queryWrapper)){
            mapper.insert(item);
        }else{
            mapper.update(item, queryWrapper);
        }
    }

    public void SynchronizeWithLeader(String entry, String logfile) {
        LogParser logParser = new LogParser(logfile);
        // write ahead log
        logParser.writeEntry(entry);
        String[] splits = entry.split(";");
        for(String seg: splits){
            String[] items = seg.split("=");
            writeItem(items[0], Integer.parseInt(items[1]));
        }
    }
}
