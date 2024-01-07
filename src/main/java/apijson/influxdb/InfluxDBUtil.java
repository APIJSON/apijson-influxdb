/*Copyright ©2024 APIJSON(https://github.com/APIJSON)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package apijson.influxdb;

import apijson.JSONResponse;
import apijson.NotNull;
import apijson.RequestMethod;
import apijson.StringUtil;
import apijson.orm.AbstractParser;
import apijson.orm.SQLConfig;
import com.alibaba.fastjson.JSONObject;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import static apijson.orm.AbstractSQLExecutor.KEY_RAW_LIST;


/**
 * @author Lemon
 * @see DemoSQLExecutor 重写 execute 方法：
 *     \@Override
 *      public JSONObject execute(@NotNull SQLConfig<Long> config, boolean unknownType) throws Exception {
 *          if (config.isMilvus()) {
 *              return MilvusUtil.execute(config, null, unknownType);
 *          }
 *
 *          return super.execute(config, unknownType);
 *     }
 */
public class InfluxDBUtil {
    public static final String TAG = "MilvusUtil";

    public static <T> JSONObject execute(@NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        if (RequestMethod.isQueryMethod(config.getMethod())) {
            List<JSONObject> list = executeQuery(config, sql, unknownType);
            JSONObject result = list == null || list.isEmpty() ? null : list.get(0);
            if (result == null) {
                result = new JSONObject(true);
            }

            if (list != null && list.size() > 1) {
                result.put(KEY_RAW_LIST, list);
            }

            return result;
        }

        return executeUpdate(config, sql);
    }

    public static <T> int execUpdate(SQLConfig<T> config, String sql) throws Exception {
        JSONObject result = executeUpdate(config, sql);
        return result.getIntValue(JSONResponse.KEY_COUNT);
    }

    public static <T> JSONObject executeUpdate(SQLConfig<T> config, String sql) throws Exception {
        InfluxDB influxDB = InfluxDBFactory.connect(config.getDBUri(), config.getDBAccount(), config.getDBPassword());
        influxDB.setDatabase(config.getSchema());

        influxDB.enableBatch(
                BatchOptions.DEFAULTS
                        .threadFactory(runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            return thread;
                        })
        );

        Runtime.getRuntime().addShutdownHook(new Thread(influxDB::close));

        influxDB.write(StringUtil.isEmpty(sql) ? config.getSQL(false) : sql);

        JSONObject result = AbstractParser.newSuccessResult();

        RequestMethod method = config.getMethod();
        if (method == RequestMethod.POST) {
            List<List<Object>> values = config.getValues();
            result.put(JSONResponse.KEY_COUNT, values == null ? 0 : values.size());
        } else {
            String idKey = config.getIdKey();
            Object id = config.getId();
            Object idIn = config.getIdIn();
            if (id != null) {
                result.put(idKey, id);
            }
            if (idIn != null) {
                result.put(idKey + "[]", idIn);
            }

            if (method == RequestMethod.PUT) {
                Map<String, Object> content = config.getContent();
                result.put(JSONResponse.KEY_COUNT, content == null ? 0 : content.size());
            } else {
                result.put(JSONResponse.KEY_COUNT, id == null && idIn instanceof Collection ? ((Collection<?>) idIn).size() : 1); // FIXME 直接 SQLAuto 传 Flux/InfluxQL INSERT 如何取数量？
            }
        }

        return result;
    }

    public static <T> List<JSONObject> executeQuery(@NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        InfluxDB influxDB = InfluxDBFactory.connect(config.getDBUri(), config.getDBAccount(), config.getDBPassword());
        influxDB.setDatabase(config.getSchema());
        QueryResult qr = influxDB.query(new Query(StringUtil.isEmpty(sql) ? config.getSQL(false) : sql));

        String err = qr == null ? null : qr.getError();
        if (StringUtil.isNotEmpty(err, true)) {
            throw new SQLException(err);
        }

        List<QueryResult.Result> list = qr == null ? null : qr.getResults();
        if (list == null) {
            return null;
        }

        List<JSONObject> resultList = new ArrayList<>();

        for (int i = 0; i < list.size(); i++) {
            QueryResult.Result qyrt = list.get(i);
            List<QueryResult.Series> seriesList = qyrt.getSeries();
            if (seriesList == null || seriesList.isEmpty()) {
                continue;
            }

            for (int j = 0; j < seriesList.size(); j++) {
                QueryResult.Series series = seriesList.get(j);
                List<List<Object>> valuesList = series.getValues();
                if (valuesList == null || valuesList.isEmpty()) {
                    continue;
                }

                List<String> columns = series.getColumns();
                for (int k = 0; k < valuesList.size(); k++) {

                    List<Object> values = valuesList.get(k);
                    JSONObject obj = new JSONObject(true);
                    if (values != null) {
                        for (int l = 0; l < values.size(); l++) {
                            obj.put(columns.get(l), values.get(l));
                        }
                    }

                    resultList.add(obj);
                }
            }
        }

        return resultList;
    }

}
