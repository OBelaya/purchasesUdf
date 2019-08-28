import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CountryByIp extends GenericUDF implements Serializable {

    private static final Logger log = Logger.getLogger(CountryByIp.class);

    //array[3]<country, minIpNum, maxIpNum>
    private List<String[]> geoData = null;
    private String currentGeoDataPath = null;

    private PrimitiveObjectInspector inputIp;
    private PrimitiveObjectInspector inputGeoData;

    private PrimitiveObjectInspector outputCountry;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        assert(objectInspectors.length == 2);
        assert(objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
        assert(objectInspectors[1].getCategory() == ObjectInspector.Category.PRIMITIVE);
        inputIp  = (PrimitiveObjectInspector)objectInspectors[0];
        assert(inputIp.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);
        inputGeoData  = (PrimitiveObjectInspector)objectInspectors[1];
        assert(inputGeoData.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);

        outputCountry = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return outputCountry;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        log.info("Udf to extract country from ip address: ");
        if (deferredObjects.length != 2) return null;
        Object ipObj = deferredObjects[0].get();
        Object geoDataPathObj = deferredObjects[1].get();

        if (ipObj == null || geoDataPathObj == null) return null;

        String ip = (String) inputIp.getPrimitiveJavaObject(ipObj);
        String geoDataPath = (String) inputGeoData.getPrimitiveJavaObject(geoDataPathObj);


        if (geoData == null || !currentGeoDataPath.equals(geoDataPath)) {
            loadGeoDataFile(geoDataPath);
        }

        return getCountryByIp(ip);
    }

    public String getDisplayString(String[] strings) {
        return "This udf returns country name of input ip address.";
    }

    private void loadGeoDataFile(String path) {
        geoData = new ArrayList<>();
        currentGeoDataPath = path;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.setBoolean("fs.file.impl.disable.cache", true);
            FileSystem fs = FileSystem.newInstance(conf);
            FSDataInputStream inputStream = fs.open(new Path(path));
            LineIterator iterator = IOUtils.lineIterator(inputStream, "UTF-8");
            String[] lineValues;
            while (iterator.hasNext()) {
                lineValues = iterator.next().split(",");
                Long minIpNum = extractMinIpNumber(lineValues[0]);
                Long maxIpNum = extractMaxIpNumber(lineValues[0]);
                log.info("min ip = " + minIpNum + "; max ip = " + maxIpNum + "; country = " + lineValues[1]);
                geoData.add(new String[]{lineValues[1], minIpNum.toString(), maxIpNum.toString()});
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getCountryByIp(String ip) {
        Long ipNum = ipToNum(ip);
        return findCountryWithBinarySearch(0, geoData.size() - 1, ipNum);
    }

    private String findCountryWithBinarySearch(int begin, int end, Long ipNum) {
        if (begin > end) {
            return null;
        }

        int pos = begin + (end - begin + 1) / 2;
        log.info("pos = " + pos);
        String[] countryIpLimits = geoData.get(pos);
        log.info("countryIpLimits array = " + countryIpLimits[0] + "; " + countryIpLimits[1] + "; " + countryIpLimits[2]);

        if (ipNum >= Long.parseLong(countryIpLimits[1]) && ipNum <= Long.parseLong(countryIpLimits[2])) {
            return countryIpLimits[0];
        } else if (ipNum < Long.parseLong(countryIpLimits[1]))  {
            return findCountryWithBinarySearch(begin, pos - 1, ipNum);
        } else {
            return findCountryWithBinarySearch(pos + 1, end, ipNum);
        }
    }

    private Long extractMinIpNumber(String network)
    {
        SubnetUtils subnetUtils = new SubnetUtils(network);
        subnetUtils.setInclusiveHostCount(true);
        return ipToNum(subnetUtils.getInfo().getLowAddress());
    }

    private Long extractMaxIpNumber(String network)
    {
        SubnetUtils subnetUtils = new SubnetUtils(network);
        subnetUtils.setInclusiveHostCount(true);
        return ipToNum(subnetUtils.getInfo().getHighAddress());
    }

    // converts ip string to number
    private Long ipToNum(String ip) {
        String[] arr = ip.split("/")[0].split("\\.");
        return (Long.parseLong(arr[0]) << 24)
                + (Long.parseLong(arr[1]) << 16)
                + (Long.parseLong(arr[2]) << 8)
        + Long.parseLong(arr[3]);
    }


}


