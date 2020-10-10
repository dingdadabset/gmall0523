package com.atguigu.gmall.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/9/22 10:09
 */
@Description(name = "explode_json_array", value = " - explode json array .... edit by atguigu")
public class ExplodeJsonArray extends GenericUDTF {
    /**
     * 作用:
     * 1. 对输入的数据做检测
     * a: 参数个数满足
     * b: 参数的类型
     * 2.  返回一个输出类型的检测器
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
//        1. 对输入的数据做检测
        // explode_json_array(get_json_object(line, '$.actions'))
//        1.1: 参数个数满足
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if (inputFields.size() != 1) {
            throw new UDFArgumentException("explode_json_array 函数的参数个数必须是 1, 你现在传递的个是: " + inputFields.size());
        }
//        1.2 参数的类型
        ObjectInspector oi = inputFields.get(0).getFieldObjectInspector();
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE
                || !"string".equals(oi.getTypeName())) {
            throw new UDFArgumentException("explode_json_array 函数的参数类型必须是string, 你现在传递的是: " + oi.getTypeName());
        }
//        2. 返回一个输出类型的检测器
        ArrayList<String> names = new ArrayList<>();
        names.add("item");
        ArrayList<ObjectInspector> ois = new ArrayList<>();
        ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
    }

    /**
     * 处理数据
     * [{}, {} ]  => {}, {}
     *
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // explode_json_array(get_json_object(line, '$.actions'))
        String jsonArrayString = args[0].toString();
        JSONArray jsonArray = new JSONArray(jsonArrayString);
        for (int i = 0; i < jsonArray.length(); i++) {
            String obj = jsonArray.getString(i);
            String[] cols = new String[1];
            cols[0] = obj;
            // 为什么要是数组?  主要是考虑, 炸裂之后, 每行会有可能是多列
            forward(cols);  // forward一次, 炸裂得到一行新数据.
        }
    }

    /**
     * 关闭资源
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }
}
