package utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CustomKey implements WritableComparable<CustomKey> {
    private Integer cityId;
    private Integer dataSetType;


    public Integer getCityId() {
        return cityId; }

    public CustomKey() {

    }

    public CustomKey(Integer userId, Integer dataSetType) {
        super();
        this.cityId = userId;
        this.dataSetType = dataSetType;
    }

    public Integer getDataSetType() {
        return dataSetType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cityId);
        out.writeInt(dataSetType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityId = in.readInt();
        dataSetType = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataSetType == null) ? 0 : dataSetType.hashCode());
        result = prime * result + ((cityId == null) ? 0 : cityId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CustomKey other = (CustomKey) obj;
        if (dataSetType == null) {
            if (other.dataSetType != null)
                return false;
        } else if (!dataSetType.equals(other.dataSetType))
            return false;
        if (cityId == null) {
            if (other.cityId != null)
                return false;
        } else if (!cityId.equals(other.cityId))
            return false;
        return true;
    }

    @Override
    public int compareTo(CustomKey o) {
        int returnValue = compare(cityId, o.getCityId());
        if (returnValue != 0) {
            return returnValue;
        }
        return compare(dataSetType, o.getDataSetType());
    }

    public static int compare(int k1, int k2) {
        return (k1 < k2 ? -1 : (k1 == k2 ? 0 : 1));
    }

    @Override
    public String toString() {
        return "util.CustomKey [userId=" + cityId + ", dataSetType=" + dataSetType + "]";
    }
}